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
    Map<Integer, BlockingQueue<SerializedPage>> channels = new HashMap<>();

    public BroadcastStream(String id, PagesSerde serde)
    {
        super(id, serde);
        this.serde = serde;
    }

    @Override
    public void write(Page page)
    {
        for (BlockingQueue<SerializedPage> queue : channels.values()) {
            queue.offer(serde.serialize(page));
        }
    }

    public SerializedPage take(int channelId)
    {
        return channels.get(channelId).poll();
    }

    public void addChannels(List<Integer> channelIds)
    {
        for (int channelId : channelIds) {
            channels.put(channelId, new LinkedBlockingQueue<>());
        }
    }
}
