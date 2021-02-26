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
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static nova.hetu.shuffle.stream.Constants.EOS;
import static nova.hetu.shuffle.stream.Constants.MAX_QUEUE_SIZE;
import static nova.hetu.shuffle.stream.PageSplitterUtil.splitPage;

/**
 * must be used in a the following way to ensure proper handling of releasing the resources
 * try (Out out = ShuffleService.getOutStream(task)) {
 * out.write(page);
 * }
 */
public class BasicStream
        implements Stream
{
    private static final Logger log = Logger.getLogger(BasicStream.class);

    private final BlockingQueue<SerializedPage> queue = new ArrayBlockingQueue<>(MAX_QUEUE_SIZE /** shuffle.grpc.buffer_size_in_item */);
    private final Set<Integer> addedChannels = new HashSet<>();
    private final Set<Integer> channels = new HashSet<>();

    private final PagesSerde serde;
    private final String id;
    private PagesSerde.CommunicationMode commMode = PagesSerde.CommunicationMode.INMEMORY;

    private AtomicBoolean eos = new AtomicBoolean(false); // endOfStream
    private boolean channelsAdded;
    private Consumer<Boolean> streamDestroyHandler;
    private final int maxPageSizeInBytes;

    public BasicStream(String id, PagesSerde serde, int maxPageSizeInBytes)
    {
        this.id = id;
        this.serde = serde;
        this.maxPageSizeInBytes = maxPageSizeInBytes;
        StreamManager.putIfAbsent(id, this);
    }

    @Override
    public SerializedPage take()
            throws InterruptedException
    {
        return queue.take();
    }

    @Override
    public void setCommunicationMode()
    {
        commMode = PagesSerde.CommunicationMode.UCX;
    }

    @Override
    public SerializedPage take(int channelId)
            throws InterruptedException
    {
        return take();
    }

    /**
     * write out the page synchronously
     *
     * @param page
     */
    @Override
    public void write(Page page)
            throws InterruptedException
    {
        if (eos.get()) {
            throw new IllegalStateException("Stream has already been closed");
        }
        for (Page splittedPage : splitPage(page, this.maxPageSizeInBytes)) {
            SerializedPage serializedPage = PageSerializeUtil.serialize(serde, splittedPage, commMode);
            // we can only acquire the page if its an offHeap page
            serializedPage.acquire();
            queue.put(serializedPage);
        }
    }

    @Override
    public void addChannels(List<Integer> channelIds, boolean noMoreChannels)
            throws InterruptedException
    {
        if (channelsAdded) {
            return;
        }

        List<Integer> newChannels = channelIds.stream().filter(channelId -> !addedChannels.contains(channelId)).collect(Collectors.toList());
        if (newChannels.isEmpty()) {
            // streams all finish but haven't received noMoreChannels
            // so we need to destroy the main stream here
            if (noMoreChannels) {
                channelsAdded = true;
                if (channels.isEmpty() && isClosed()) {
                    destroy();
                }
            }
            return;
        }

        synchronized (channels) {
            for (Integer channelId : channelIds) {
                if (addedChannels.contains(channelId)) {
                    continue;
                }

                String streamId = id + "-" + channelId;
                StreamManager.putIfAbsent(streamId, new ReferenceStream(channelId, streamId, this));
                channels.add(channelId);
                addedChannels.add(channelId);
                log.info("Stream " + id + " add channel " + channelId);
                if (eos.get()) {
                    queue.put(EOS);
                }
            }
        }

        if (noMoreChannels) {
            channelsAdded = true;
        }
    }

    @Override
    public boolean isClosed()
    {
        // When there are multiple channels, stream should only
        // be closed when all channels are added
        if (!addedChannels.isEmpty() && !channelsAdded) {
            return false;
        }
        return eos.get() && queue.isEmpty();
    }

    @Override
    public void destroy()
    {
        log.info("Stream " + id + " destroyed");
        StreamManager.remove(id);
        if (streamDestroyHandler != null) {
            streamDestroyHandler.accept(true);
        }
    }

    @Override
    public void destroyChannel(int channelId)
    {
        log.info("Stream " + id + " channel " + channelId + " destroyed");
        synchronized (channels) {
            channels.remove(channelId);
        }
        if (channels.isEmpty() && channelsAdded) {
            destroy();
        }
    }

    @Override
    public void onDestroyed(Consumer<Boolean> streamDestroyHandler)
    {
        this.streamDestroyHandler = streamDestroyHandler;
    }

    @Override
    public void close()
            throws InterruptedException
    {
        if (!eos.get()) {
            if (eos.compareAndSet(false, true)) {
                log.info("Closing Stream " + id);
                if (addedChannels.isEmpty()) {
                    log.info("Adding EOS to " + id);
                    queue.put(EOS);
                    return;
                }
                synchronized (channels) {
                    for (int i = 0; i < channels.size(); i++) {
                        log.info("Adding EOS to " + id + "-" + i);
                        queue.put(EOS);
                    }
                }
            }
        }
    }

    @Override
    public String toString()
    {
        return id;
    }
}
