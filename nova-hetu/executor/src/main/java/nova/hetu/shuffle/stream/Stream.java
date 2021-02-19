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

public interface Stream
        extends AutoCloseable
{
    /**
     * Basic Stream supports exactly once data transportation, all the consumers
     * work as first come first served pattern.
     * <p>
     * Broadcast Stream supports reliable broadcast transportation to all consumers,
     * all consumers should get exactly the same data.
     */
    enum Type
    {
        BASIC,
        BROADCAST,
    }

    /**
     * Take a page from the stream, when the stream is empty, this operation will
     * wait for new pages.
     *
     * @return First SerializedPage in the stream
     * @throws InterruptedException Exception when the waiting thread gets interrupted
     */
    SerializedPage take()
            throws InterruptedException;

    /**
     * Take a page from an established channel of the stream, when the channel is empty,
     * this operation will wait for new pages.
     *
     * @param channelId channel to take pages from
     * @return First SerializedPage in the channel
     * @throws InterruptedException Exception when the waiting thread gets interrupted
     */
    SerializedPage take(int channelId)
            throws InterruptedException;

    /**
     * Write a page to the end of the stream, when the stream is full, this operation
     * will be blocked wait for the stream to have some space.
     *
     * @param page Page that needs to be sent by the stream
     * @throws InterruptedException Exception when the waiting thread gets interrupted
     */
    void write(Page page)
            throws InterruptedException;

    /**
     * Please refer to {@link nova.hetu.shuffle.stream.Stream#addChannels(List, boolean)}
     * this should be used when channels are only added in one batch.
     *
     * @param channelIds unique integer IDs of the channels to be established
     * @throws InterruptedException Exception when the waiting thread gets interrupted
     */
    default void addChannels(List<Integer> channelIds)
            throws InterruptedException
    {
        addChannels(channelIds, true);
    }

    /**
     * Establish new channels to the stream so the data can be consumed by different clients
     * If there are multiple channels to be added, the stream won't be closed until noMoreChannels
     * is set.
     *
     * @param channelIds unique integer IDs of the channels to be established
     * @param noMoreChannels tells the stream if there are more channels to be established
     * @throws InterruptedException Exception when the waiting thread gets interrupted
     */
    void addChannels(List<Integer> channelIds, boolean noMoreChannels)
            throws InterruptedException;

    /**
     * Set the communication mode
     */
    void setCommunicationMode();

    /**
     * Check if the stream is closed
     *
     * @return if the stream is closed
     */
    boolean isClosed();

    /**
     * Destroy the current stream
     */
    void destroy();

    /**
     * Destroy a channel of the stream.
     *
     * @param channelId channel to be destroyed
     */
    void destroyChannel(int channelId);

    /**
     * Register a handler function that can be triggered when the stream gets destroyed
     *
     * @param streamDestroyHandler handler function to be called when stream gets destroyed
     */
    void onDestroyed(Consumer<Boolean> streamDestroyHandler);
}
