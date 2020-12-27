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

import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.slice.Slices.EMPTY_SLICE;

/**
 * must be used in a the following way to ensure proper handling of releasing the resources
 * try (Out out = ShuffleService.getOutStream(task)) {
 * out.write(page);
 * }
 */
public class Stream
        implements AutoCloseable
{
    public static final SerializedPage EOS = new SerializedPage(EMPTY_SLICE, PageCodecMarker.MarkerSet.empty(), 0, 0);
    private static Logger log = Logger.getLogger(Stream.class);
    static ConcurrentHashMap<String, Stream> streamMap = new ConcurrentHashMap<>();

    private final PagesSerde serde;
    ArrayBlockingQueue<SerializedPage> queue = new ArrayBlockingQueue(1000 /** shuffle.grpc.buffer_size_in_item */);
    String id;
    boolean eos; // endOfStream

    /**
     * Returns a OutStream which will be used PRODUCER to sent the data to be returned to service caller
     *
     * @return
     */
    public static Stream create(String producerId, PagesSerde serde, PageProducer.Type type)
    {
        log.info("Creating output stream for: " + producerId);
        Stream out = streamMap.get(producerId);
        if (out == null) {
            out = new StreamFactory().create(producerId, serde, type);
            Stream temp = streamMap.putIfAbsent(producerId, out);
            out = temp != null ? temp : out;
        }
        return out;
    }

    public static Stream get(String producerId)
    {
        log.info("Getting output stream for: " + producerId);
        return streamMap.get(producerId);
    }

    Stream(String id, PagesSerde serde)
    {
        this.id = id;
        this.serde = serde;
    }

    public SerializedPage take()
            throws InterruptedException
    {
        return queue.take();
    }

    /**
     * write out the page synchronously
     *
     * @param page
     */
    public void write(Page page)
            throws InterruptedException
    {
        if (eos) {
            throw new IllegalStateException("Output stream is closed already");
        }
        queue.put(serde.serialize(page));
    }

    public void addChannels(List<Integer> channelIds)
    {
    }

    public boolean isClosed()
    {
        return eos && queue.isEmpty();
    }

    @Override
    public void close()
            throws Exception
    {
        eos = true;
        queue.put(EOS);
    }

    static void destroy(Stream stream)
    {
        streamMap.remove(stream.id);
    }
}
