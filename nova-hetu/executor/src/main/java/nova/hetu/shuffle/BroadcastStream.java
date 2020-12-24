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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BroadcastStream
        extends Stream
{
    PagesSerde serde;
    BlockingQueue<SerializedPage> initialPages = new LinkedBlockingQueue<>();
    Map<Integer, BlockingQueue<SerializedPage>> channels = new HashMap<>();

    public BroadcastStream(String id, PagesSerde serde)
    {
        super(id, serde);
        this.serde = serde;
    }

    @Override
    public void write(Page page)
            throws InterruptedException
    {
        SerializedPage serializedPage = serde.serialize(page);
        if (channels.isEmpty()) {
            initialPages.put(serializedPage);
            return;
        }

        for (BlockingQueue<SerializedPage> queue : channels.values()) {
            queue.offer(serializedPage);
        }
    }

    public SerializedPage take(int channelId)
            throws InterruptedException
    {
        BlockingQueue<SerializedPage> channel = channels.get(channelId);
        if (channel == null) {
            return null;
        }
        return channel.take();
    }

    public void addChannels(List<Integer> channelIds)
    {
        for (int channelId : channelIds) {
            channels.put(channelId, new LinkedBlockingQueue<>());
            streamMap.put(id + "-" + channelId, this);
        }
        streamMap.remove(id);
        while (!initialPages.isEmpty()) {
            SerializedPage page = initialPages.poll();
            channels.values().forEach(channel -> {
                try {
                    channel.put(page);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    @Override
    public boolean isClosed()
    {
        return eos && queue.isEmpty();
    }

    @Override
    public void close()
            throws Exception
    {
        eos = true;
        for (BlockingQueue<SerializedPage> channel : channels.values()) {
            channel.put(EOS);
        }
    }

    static void destroy(Stream stream)
    {
        streamMap.remove(stream.id);
    }
}
