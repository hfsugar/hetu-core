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
package nova.hetu.executor;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;

import java.util.List;

public class PageProducer
{
    private final Stream stream;
    private final Type type;

    public enum Type
    {
        PARTITIONED,
        BROADCAST,
        ARBITRARY,
    }

    PageProducer(Stream stream, Type type)
    {
        this.stream = stream;
        this.type = type;
    }

    public static PageProducer create(String producerId, PagesSerde serde/** can we hide this? */)
    {
        return create(producerId, serde, Type.PARTITIONED);
    }

    /**
     * Creates a Page Producer for sending pages to a producerId of a specific partition, when the output is not parititoned
     * the default partitionid is 0
     *
     * @param producerId
     * @param serde
     * @param type
     * @return
     */
    public static PageProducer create(String producerId, PagesSerde serde, /** can we hide this? */Type type)
    {
        Stream stream = ShuffleService.getStream(producerId, serde, type);
        return new PageProducer(stream, type);
    }

    public void send(Page page)
            throws InterruptedException
    {
        stream.write(page);
    }

    public void addConsumers(List<Integer> newConsumers)
    {
        if (type != Type.PARTITIONED) {
            stream.addChannels(newConsumers);
        }
    }

    public void setNoMoreConsumers()
    {
        stream.setNoMoreChannels();
    }

    public void close()
            throws Exception
    {
        stream.close();
    }

    public boolean isClosed()
    {
        return stream.isClosed();
    }
}
