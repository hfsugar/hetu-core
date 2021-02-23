/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import nova.hetu.ShuffleServiceConfig;
import nova.hetu.shuffle.PageConsumer;
import nova.hetu.shuffle.ProducerInfo;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * One client per location/split.
 * The client ensures the order of the page returned
 */
public class StreamingExchangeClient
        implements ExchangeClient
{
    private static final int MAX_PAGE_OUTPUT_BUFFER_SIZE = 1000;
    private final long bufferCapacity;
    private final int concurrentRequestMultiplier;

    @GuardedBy("this")
    private long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long maxBufferRetainedSizeInBytes;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<URI, PageConsumer> pageConsumers = new ConcurrentHashMap<>();
    private final Set<PageConsumer> activePageConsumers = new HashSet<>();
    private final Set<PageConsumer> allPageConsumers = new HashSet<>();
    private final PagesSerde.CommunicationMode defaultTransType;
    private final List<SettableFuture<?>> blockedCallers = new ArrayList<>();
    private final ExecutorService executorService = Executors.newFixedThreadPool(1);
    private LinkedBlockingDeque<Page> pageOutputBuffer = new LinkedBlockingDeque<>();
    private ScheduledExecutorService scheduler;
    private boolean noMoreLocation;
    private Throwable failed;

    private final LocalMemoryContext systemMemoryContext;
    private final PagesSerde pagesSerde;
    private int nPolledPages;
    private boolean dynamicRateLimit;

    public StreamingExchangeClient(DataSize bufferCapacity,
            int concurrentRequestMultiplier,
            LocalMemoryContext systemMemoryContext,
            PagesSerde pagesSerde,
            ShuffleServiceConfig.TransportType transportType,
            ScheduledExecutorService scheduler)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.bufferCapacity = bufferCapacity.toBytes();
        this.systemMemoryContext = systemMemoryContext;
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.maxBufferRetainedSizeInBytes = Long.MIN_VALUE;
        this.bufferRetainedSizeInBytes = 0;
        this.nPolledPages = 0;
        this.defaultTransType = (transportType == ShuffleServiceConfig.TransportType.UCX ? PagesSerde.CommunicationMode.UCX : PagesSerde.CommunicationMode.RSOCKET);
        this.dynamicRateLimit = false;
        this.scheduler = scheduler;
        executorService.execute(() -> {
                while (!isFinished() && !closed.get()) {
                    try {
                        if (pageOutputBuffer.size() < MAX_PAGE_OUTPUT_BUFFER_SIZE) {
                            pollPageFromPageConsumers();
                        }
                        else {
                            Thread.sleep(2);
                        }
                    }
                    catch (InterruptedException e) {
                        // ignore interrupted exception
                    }
                    catch (Exception e) {
                        if (failed == null) {
                            failed = e;
                        }
                        break;
                    }
                }
            }
        );
    }

    @Override
    public ExchangeClientStatus getStatistics()
    {
        return null;
    }

    @Override
    public synchronized void addLocation(URI location)
    {
        //location URI format: /v1/task/{taskId}/results/{bufferId}/{token} --> ["", "v1", "task",{taskid}, "result", {bufferid}]
        if (!pageConsumers.containsKey(location)) {
            PageConsumer pageConsumer = PageConsumer.create(new ProducerInfo(location), pagesSerde, defaultTransType, false);
            pageConsumers.put(location, pageConsumer);
            synchronized (activePageConsumers) {
                allPageConsumers.add(pageConsumer);
                activePageConsumers.add(pageConsumer);
            }
        }
    }

    @Override
    public void setNoMoreLocation()
    {
        noMoreLocation = true;
    }

    @Nullable
    @Override
    public synchronized Page pollPage()
    {
        Page page = null;
        if (failed != null) {
            throw new RuntimeException(failed);
        }
        page = pageOutputBuffer.poll();

        if (page == null) {
            return null;
        }

        if (dynamicRateLimit && nPolledPages == 0) {
            increaseOrDecreaseRateLimit();
        }
        nPolledPages = (nPolledPages + 1) % 100;

        synchronized (this) {
            if (dynamicRateLimit && !closed.get()) {
                long pageRetainedSizeInBytes = page.getRetainedSizeInBytes();
                if (bufferRetainedSizeInBytes > pageRetainedSizeInBytes) {
                    bufferRetainedSizeInBytes -= pageRetainedSizeInBytes;
                    systemMemoryContext.setBytes(bufferRetainedSizeInBytes);
                }
            }
        }

        return page;
    }

    private void pollPageFromPageConsumers() throws Exception
    {
        boolean needNotifyBlockedCallers = false;
        if (allPageConsumers.isEmpty()) {
            notifyBlockedCallers();
            return;
        }

        synchronized (activePageConsumers) {
            for (PageConsumer pageConsumer : activePageConsumers) {
                if (pageConsumer.isEnded()) {
                    if (allPageConsumers.contains(pageConsumer)) {
                        allPageConsumers.remove(pageConsumer);
                    }
                    needNotifyBlockedCallers = true;
                    continue;
                }

                Page page = pageConsumer.poll();
                if (page != null) {
                    pageOutputBuffer.put(page);
                    needNotifyBlockedCallers = true;
                }
            }
        }

        if (needNotifyBlockedCallers) {
            notifyBlockedCallers();
        }
    }

    @Override
    public boolean isFinished()
    {
        return noMoreLocation && allPageConsumers.isEmpty() && pageOutputBuffer.isEmpty();
    }

    @Override
    public boolean isClosed()
    {
        /** shouldn't need to check if pageConsumer is null if this is only called after {@link #addLocation(URI)} is invoked */
        return closed.get() || isFinished();
    }

    @Override
    public synchronized ListenableFuture<?> isBlocked()
    {
        if (isClosed() || !pageOutputBuffer.isEmpty()) {
            return Futures.immediateFuture(true);
        }

        SettableFuture<?> future = SettableFuture.create();
        blockedCallers.add(future);
        return future;
    }

    private synchronized void notifyBlockedCallers()
    {
        if (blockedCallers.isEmpty()) {
            return;
        }

        List<SettableFuture<?>> callers = ImmutableList.copyOf(blockedCallers);
        blockedCallers.clear();
        for (SettableFuture<?> blockedCaller : callers) {
            scheduler.execute(() -> blockedCaller.set(null));
        }
    }

    @Override
    public void close()
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (PageConsumer client : pageConsumers.values()) {
            closeQuietly(client);
        }
        systemMemoryContext.setBytes(0);
        bufferRetainedSizeInBytes = 0;
    }

    private static void closeQuietly(PageConsumer client)
    {
        try {
            client.close();
        }
        catch (RuntimeException | IOException e) {
            // ignored
        }
    }

    public synchronized void increaseOrDecreaseRateLimit()
    {
        if (isFinished()) {
            return;
        }

        bufferRetainedSizeInBytes = 0;
        int numConsumers = 0;
        int rateLimit = 0;
        long numPages = 0;
        //long responseSize = 0;
        for (PageConsumer pageConsumer : activePageConsumers) {
            if (!pageConsumer.isEnded()) {
                numPages += pageConsumer.getPageOutputBufferSize();
                bufferRetainedSizeInBytes += pageConsumer.getTotalPagesRetainedSizeInBytes();
                //responseSize += pageConsumer.getTotalSizeInBytes();
                rateLimit += pageConsumer.getRateLimit();
                numConsumers++;
            }
        }
        maxBufferRetainedSizeInBytes = Math.max(maxBufferRetainedSizeInBytes, bufferRetainedSizeInBytes);
        systemMemoryContext.setBytes(bufferRetainedSizeInBytes);

        //successfulRequests++;
        //averageBytesPerRequest = (long) (1.0 * averageBytesPerRequest * (successfulRequests - 1) / successfulRequests + responseSize / successfulRequests);

        numPages = numPages / numConsumers;
        rateLimit = rateLimit / numConsumers;

        long neededBytes = bufferCapacity - bufferRetainedSizeInBytes;
        // Decrease rate limit
        if (neededBytes <= 0) {
            for (PageConsumer pageConsumer : activePageConsumers) {
                if (!pageConsumer.isEnded()) {
                    pageConsumer.changeRateLimit(-rateLimit / 2);
                    System.out.println("Decreasing rate limit from " + rateLimit + " to " + rateLimit / 2);
                }
            }
            return;
        }

        // Potentially increase rate limit
        long averageRetainedSizeInBytes = bufferRetainedSizeInBytes / numPages;
        int incRateLimit = (int) (neededBytes / averageRetainedSizeInBytes / rateLimit / numConsumers);

        for (PageConsumer pageConsumer : activePageConsumers) {
            if (!pageConsumer.isEnded()) {
                pageConsumer.changeRateLimit(incRateLimit);
                System.out.println("Increasing rate limit from " + rateLimit + " to " + incRateLimit);
            }
        }

        //int clientCount = (int) ((1.0 * neededBytes / averageBytesPerRequest) * concurrentRequestMultiplier);
        //clientCount = Math.max(clientCount, 1);
    }
}
