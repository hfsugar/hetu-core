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
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;
import nova.hetu.shuffle.PageConsumer;
import nova.hetu.shuffle.ProducerInfo;

import javax.annotation.Nullable;

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
    private final PagesSerde pagesSerde;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final ConcurrentHashMap<URI, PageConsumer> pageConsumers = new ConcurrentHashMap<>();
    private final Queue<PageConsumer> activePageConsumers = new LinkedList<>();
    private boolean noMoreLocation;

    public StreamingExchangeClient(PagesSerde pagesSerde)
    {
        this.pagesSerde = requireNonNull(pagesSerde, "pagesSerde is null");
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
            PageConsumer pageConsumer = PageConsumer.create(new ProducerInfo(location), pagesSerde, false);
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
}
