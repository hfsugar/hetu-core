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
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class StreamManager
{
    public static final long DEFAULT_MAX_WAIT = 5000;
    public static final long DEFAULT_SLEEP_INTERVAL = 50;
    private static final Logger LOG = Logger.getLogger(StreamManager.class);
    private static final ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<>();

    private StreamManager() {}

    public static Stream getStream(String producerId, PagesSerde.CommunicationMode mode, long maxWait, long sleepInterval)
    {
        Stream stream = StreamManager.get(producerId, mode);

        while (stream == null && maxWait > 0) {
            stream = StreamManager.get(producerId, mode);
            try {
                maxWait -= sleepInterval;
                Thread.sleep(sleepInterval);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (stream != null) {
                LOG.info("Got stream after retry " + producerId);
            }
        }
        return stream;
    }

    public static Stream get(String streamId, PagesSerde.CommunicationMode commMode)
    {
        LOG.info("Getting stream for: " + streamId);
        Stream stream = streams.get(streamId);
        if (stream != null && commMode != PagesSerde.CommunicationMode.INMEMORY) {
            stream.setCommunicationMode();
        }
        return stream;
    }

    public static Stream put(String streamId, Stream stream)
    {
        return streams.put(streamId, stream);
    }

    public static Stream putIfAbsent(String streamId, Stream stream)
    {
        return streams.putIfAbsent(streamId, stream);
    }

    public static Stream remove(String streamId)
    {
        return streams.remove(streamId);
    }
}
