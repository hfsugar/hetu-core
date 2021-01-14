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
import nova.hetu.shuffle.rsocket.RsShuffleClient;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class PageConsumer
{
    LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
    PagesSerde serde;
    private final AtomicBoolean shuffleClientFinished = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    public static PageConsumer create(ProducerInfo producerInfo, PagesSerde serde)
    {
        return new PageConsumer(producerInfo, serde);
    }

    PageConsumer(ProducerInfo producerInfo, PagesSerde serde)
    {
        this.pageOutputBuffer = new LinkedBlockingQueue<>();
        this.serde = serde;

        // TODO: pass in an event listener to handler success and failure events
        RsShuffleClient.getResults(producerInfo.getHost(), producerInfo.getPort(), producerInfo.getProducerId(), pageOutputBuffer, new ShuffleClientCallbackImpl());
//        future = ShuffleClient.getResults(producerInfo.getHost(), producerInfo.getPort(), producerInfo.getProducerId(), pageOutputBuffer);
    }

    public Page poll()
    {
        Throwable t = failure.get();
        if (t != null) {
            throw new RuntimeException(t);
        }
        SerializedPage page = pageOutputBuffer.poll();
        if (page == null) {
            return null;
        }
        return serde.deserialize(page);
    }

    public boolean isEnded()
    {
        return pageOutputBuffer.isEmpty() && shuffleClientFinished.get();
    }

    private class ShuffleClientCallbackImpl
            implements ShuffleClientCallback
    {
        @Override
        public void clientFinished()
        {
            shuffleClientFinished.compareAndSet(false, true);
        }

        @Override
        public void clientFailed(Throwable cause)
        {
            requireNonNull(cause, "cause is null");
            if (!isEnded()) {
                failure.compareAndSet(null, cause);
            }
        }
    }
}
