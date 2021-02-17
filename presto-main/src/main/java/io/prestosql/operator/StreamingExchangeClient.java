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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
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
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

/**
 * One client per location/split.
 * The client ensures the order of the page returned
 */
public class StreamingExchangeClient
        implements ExchangeClient
{
    private final long bufferCapacity;
    private final int concurrentRequestMultiplier;

    @GuardedBy("this")
    private long bufferRetainedSizeInBytes;
    @GuardedBy("this")
    private long maxBufferRetainedSizeInBytes;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<URI, PageConsumer> pageConsumers = new ConcurrentHashMap<>();
    private final Queue<PageConsumer> activePageConsumers = new LinkedList<>();
    private final PagesSerde.CommunicationMode defaultTransType;
    private boolean noMoreLocation;

    private final LocalMemoryContext systemMemoryContext;
    private final PagesSerde pagesSerde;
    int nPolledPages;

    public StreamingExchangeClient(DataSize bufferCapacity,
            int concurrentRequestMultiplier,
            LocalMemoryContext systemMemoryContext,
            PagesSerde pagesSerde,
            ShuffleServiceConfig.TransportType transportType)
    {
        this.concurrentRequestMultiplier = concurrentRequestMultiplier;
        this.bufferCapacity = bufferCapacity.toBytes();
        this.systemMemoryContext = systemMemoryContext;
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
        this.maxBufferRetainedSizeInBytes = Long.MIN_VALUE;
        this.nPolledPages = 0;
        this.defaultTransType = (transportType == ShuffleServiceConfig.TransportType.UCX ? PagesSerde.CommunicationMode.UCX : PagesSerde.CommunicationMode.RSOCKET);
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
            activePageConsumers.offer(pageConsumer);
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
        int numConsumers = activePageConsumers.size();
        while (page == null && numConsumers > 0) {
            PageConsumer pageConsumer = activePageConsumers.poll();
            if (!pageConsumer.isEnded()) {
                page = pageConsumer.poll();
                activePageConsumers.offer(pageConsumer);
            }
            numConsumers--;
        }

        if (page == null) {
            return null;
        }

        nPolledPages = (nPolledPages + 1) % 100;
        if (nPolledPages == 0) {
//            increaseOrDecreaseRateLimit();
        }

        synchronized (this) {
            if (!closed.get()) {
                bufferRetainedSizeInBytes -= page.getRetainedSizeInBytes();
                systemMemoryContext.setBytes(bufferRetainedSizeInBytes);
            }
        }

        return page;
    }

    @Override
    public boolean isFinished()
    {
        return noMoreLocation && activePageConsumers.isEmpty();
    }

    @Override
    public boolean isClosed()
    {
        /** shouldn't need to check if pageConsumer is null if this is only called after {@link #addLocation(URI)} is invoked */
        return closed.get() || isFinished();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return Futures.immediateFuture(true);
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
