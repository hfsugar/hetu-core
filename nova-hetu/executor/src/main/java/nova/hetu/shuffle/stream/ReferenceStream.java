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

import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.function.Consumer;

public class ReferenceStream
        implements Stream
{
    private final int channelId;
    private final String streamId;
    private final Stream stream;

    public ReferenceStream(int channelId, String streamId, Stream stream)
    {
        this.channelId = channelId;
        this.streamId = streamId;
        this.stream = stream;
    }

    @Override
    public SerializedPage take()
            throws InterruptedException
    {
        return stream.take(this.channelId);
    }

    @Override
    public SerializedPage take(int channelId)
            throws InterruptedException
    {
        return stream.take(channelId);
    }

    @Override
    public void write(Page page)
            throws InterruptedException
    {
        stream.write(page);
    }

    @Override
    public void addChannels(List<Integer> channels, boolean noMoreChannels)
            throws InterruptedException
    {
        stream.addChannels(channels, noMoreChannels);
    }

    @Override
    public boolean isClosed()
    {
        return stream.isClosed();
    }

    @Override
    public void destroy()
    {
        StreamManager.remove(streamId);
        stream.destroyChannel(this.channelId);
    }

    @Override
    public void destroyChannel(int channelId)
    {
        stream.destroyChannel(channelId);
    }

    @Override
    public void onDestroyed(Consumer<Boolean> streamDestroyHandler)
    {
        stream.onDestroyed(streamDestroyHandler);
    }

    @Override
    public void close()
            throws Exception
    {
        stream.close();
    }
}
