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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static nova.hetu.shuffle.stream.Constants.EOS;
import static nova.hetu.shuffle.stream.Constants.MAX_QUEUE_SIZE;
import static nova.hetu.shuffle.stream.PageSplitterUtil.splitPage;

public class BroadcastStream
        implements Stream
{
    private static final Logger log = Logger.getLogger(BroadcastStream.class);

    private final BlockingQueue<SerializedPage> pendingPages = new LinkedBlockingQueue<>(MAX_QUEUE_SIZE);
    private final Set<Integer> addedChannels = new HashSet<>();
    private final Map<Integer, BlockingQueue<SerializedPage>> channels = new ConcurrentHashMap<>();

    private final PagesSerde serde;
    private final String id;
    private boolean eos; // endOfStream
    private PagesSerde.CommunicationMode commMode;

    private boolean channelsAdded;
    private Consumer<Boolean> streamDestroyHandler;

    public BroadcastStream(String id, PagesSerde serde)
    {
        this.id = id;
        this.serde = serde;
        this.commMode = PagesSerde.CommunicationMode.STANDARD;
    }

    @Override
    public void setCommunicationMode() {}

    @Override
    public SerializedPage take()
    {
        throw new RuntimeException("Channel id is required for Broadcast stream");
    }

    @Override
    public SerializedPage take(int channelId)
            throws InterruptedException
    {
        BlockingQueue<SerializedPage> channel = channels.get(channelId);
        if (channel == null) {
            throw new RuntimeException("Channel doesn't exist, stream id " + id + ", channel " + channelId);
        }
        log.info("Stream " + id + " take channel " + channelId);
        return channel.take();
    }

    @Override
    public void write(Page page)
            throws InterruptedException
    {
        List<SerializedPage> serializedPages = splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                .map(serde::serialize)
                .collect(Collectors.toList());

        if (!channelsAdded) {
            for (SerializedPage splittedPage : serializedPages) {
                pendingPages.put(splittedPage);
//                log.info("Stream " + id + " write initial pages " + page);
            }
        }

        for (BlockingQueue<SerializedPage> channel : channels.values()) {
            for (SerializedPage splittedPage : serializedPages) {
                channel.put(splittedPage);
//                log.info("Stream " + id + " write channel " + channel.toString() + " page " + page);
            }
        }
    }

    @Override
    public void addChannels(List<Integer> channelIds, boolean noMoreChannels)
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
                if (channels.isEmpty()) {
                    destroy();
                }
            }
            return;
        }

        for (Integer channelId : newChannels) {
            String streamId = id + "-" + channelId;
            StreamManager.putIfAbsent(streamId, new ReferenceStream(channelId, streamId, this));
            channels.put(channelId, new LinkedBlockingQueue<>(MAX_QUEUE_SIZE));
            addedChannels.add(channelId);
            log.info("Stream " + id + " add channel " + channelId);
        }

        log.info("Stream " + id + " adding " + pendingPages.size() + " pages to channels: " + channels.keySet());
        for (SerializedPage page : pendingPages) {
            newChannels.stream().map(channels::get).forEach(channel -> {
                try {
                    channel.put(page);
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        if (noMoreChannels) {
            channelsAdded = true;
            pendingPages.clear();
        }
    }

    @Override
    public boolean isClosed()
    {
        boolean allChannelsEmpty = true;
        for (BlockingQueue<SerializedPage> channel : channels.values()) {
            if (!channel.isEmpty()) {
                allChannelsEmpty = false;
                break;
            }
        }
        return eos && channelsAdded && allChannelsEmpty;
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
        channels.remove(channelId);
        log.info("Stream " + id + " channel " + channelId + " destroyed");
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
        if (!eos) {
            if (!channelsAdded) {
                pendingPages.put(EOS);
            }

            for (BlockingQueue<SerializedPage> channel : channels.values()) {
                channel.put(EOS);
            }
            eos = true;
            log.info("Stream " + id + " closed");
        }
    }

    @Override
    public String toString()
    {
        return id;
    }
}
