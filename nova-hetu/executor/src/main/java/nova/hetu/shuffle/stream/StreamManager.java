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

import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

public class StreamManager
{
    private static final Logger LOG = Logger.getLogger(StreamManager.class);

    private StreamManager() {}

    private static ConcurrentHashMap<String, Stream> streams = new ConcurrentHashMap<>();

    public static Stream get(String streamId)
    {
        LOG.info("Getting stream for: " + streamId);
        return streams.get(streamId);
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
