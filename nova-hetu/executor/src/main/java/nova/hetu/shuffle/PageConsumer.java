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
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class PageConsumer
{
    LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
    PagesSerde serde;
    Future future;

    public static PageConsumer create(ProducerInfo producerInfo, PagesSerde serde)
    {
        return new PageConsumer(producerInfo, serde);
    }

    PageConsumer(ProducerInfo producerInfo, PagesSerde serde)
    {
        this.pageOutputBuffer = new LinkedBlockingQueue<>();
        this.serde = serde;

//        future = RsShuffleClient.getResults(producerInfo.getHost(), 7878, producerInfo.getProducerId(), pageOutputBuffer);
        future = ShuffleClient.getResults(producerInfo.getHost(), producerInfo.getPort(), producerInfo.getProducerId(), pageOutputBuffer);
    }

    public Page poll()
    {
        SerializedPage page = pageOutputBuffer.poll();
        if (page == null) {
            return null;
        }
        return serde.deserialize(page);
    }

    public boolean isEnded()
    {
        return pageOutputBuffer.isEmpty() && future.isDone();
    }
}
