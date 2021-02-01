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
package nova.hetu.shuffle.stream;

import io.hetu.core.transport.execution.buffer.PagesSerde;

public class StreamFactory
{
    private StreamFactory() {}

    public static synchronized Stream getOrCreate(String producerId, PagesSerde serde, Stream.Type type)
    {
        Stream stream = StreamManager.get(producerId, PagesSerde.CommunicationMode.INMEMORY);
        if (stream == null) {
            stream = create(producerId, serde, type);
        }
        return stream;
    }

    public static Stream create(String producerId, PagesSerde serde, Stream.Type type)
    {
        switch (type) {
            case BASIC:
                return new BasicStream(producerId, serde);
            case BROADCAST:
                return new BroadcastStream(producerId, serde);
            default:
                throw new RuntimeException("Unsupported PageProducer type: " + type);
        }
    }
}
