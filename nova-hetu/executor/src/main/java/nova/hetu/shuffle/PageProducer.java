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
package nova.hetu.shuffle;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;
import nova.hetu.shuffle.stream.Stream;
import nova.hetu.shuffle.stream.StreamFactory;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.function.Consumer;

public class PageProducer
        implements AutoCloseable
{
    public static final Logger LOG = Logger.getLogger(PageProducer.class);
    private final Stream stream;
    private boolean isClosed;

    public PageProducer(String producerId, PagesSerde pagesSerde, Stream.Type type)
    {
        LOG.info("Create or get producer " + producerId);
        this.stream = StreamFactory.getOrCreate(producerId, pagesSerde, type);
    }

    public void send(Page page)
            throws InterruptedException
    {
        stream.write(page);
    }

    public void addConsumers(List<Integer> newConsumers, boolean noMoreConsumers)
            throws InterruptedException
    {
        stream.addChannels(newConsumers, noMoreConsumers);
    }

    public void close()
            throws Exception
    {
        isClosed = true;
        stream.close();
    }

    public boolean isClosed()
    {
        return isClosed;
    }

    public void onClosed(Consumer<Boolean> handler)
    {
        stream.onDestroyed(handler);
    }
}
