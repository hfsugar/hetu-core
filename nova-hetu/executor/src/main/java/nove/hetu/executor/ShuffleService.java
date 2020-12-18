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
package nove.hetu.executor;

import com.google.inject.Inject;
import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import nova.hetu.executor.ExecutorOuterClass;
import nova.hetu.executor.ShuffleGrpc;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

import static io.airlift.slice.Slices.EMPTY_SLICE;

/**
 * A gRpc service to transfer serialized pages in a streaming manner
 * All retry, backoff capabilities will be provided by gRpc
 */
public class ShuffleService
        extends ShuffleGrpc.ShuffleImplBase
{
    private static ConcurrentHashMap<String, Stream> streamMap = new ConcurrentHashMap<>();

    private static Logger log = Logger.getLogger(ShuffleService.class);

    @Inject
    public ShuffleService() {}

    /**
     * Down stream operators call this method via gRpc to retrieve the output of the task
     *
     * @param request
     * @param responseObserver
     */
    @Override
    public void getResult(ExecutorOuterClass.Task request, StreamObserver<ExecutorOuterClass.Page> responseObserver)
    {
        log.info("====================== Get result for " + request.getTaskId() + "-" + request.getBufferId());
        ServerCallStreamObserver<ExecutorOuterClass.Page> serverCallStreamObserver = (ServerCallStreamObserver<ExecutorOuterClass.Page>) responseObserver;
        Stream stream = streamMap.get(toKey(request.getTaskId(), request.getBufferId()));
        while (stream == null && !serverCallStreamObserver.isCancelled()) {
            stream = streamMap.get(toKey(request.getTaskId(), request.getBufferId()));
            if (stream != null) {
                log.info("Got output stream after retry " + request.getTaskId() + "-" + request.getBufferId());
            }
        }

//        if (stream == null) {
//            throw new RuntimeException("invalid task: " + request.getTaskId());
//        }
        int count = 0;
        SerializedPage page;
        try {
            while (true && !serverCallStreamObserver.isCancelled()) {
                page = stream.take();
                if (page == Stream.EOS) {
                    break;
                }
                responseObserver.onNext(transform(page));
                count++;
//                log.info("pages sent: " + count);
            }
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        finally {
            streamMap.remove(stream.id);
            responseObserver.onCompleted();
            log.info("====================== Finished sending pages for " + request.getTaskId() + "-" + request.getBufferId() + " count:" + count);
        }
    }

    private static String toKey(String taskid, String bufferid)
    {
        return taskid + "/" + bufferid;
    }

    private ExecutorOuterClass.Page transform(SerializedPage page)
    {
        return ExecutorOuterClass.Page.newBuilder()
                .setSliceArray(ByteString.copyFrom(page.getSliceArray()))
                .setPageCodecMarkers(page.getPageCodecMarkers())
                .setPositionCount(page.getPositionCount())
                .setUncompressedSizeInBytes(page.getUncompressedSizeInBytes())
                .build();
    }

    /**
     * Returns a OutStream which will be used bu PRODUCER to sent the data to be returned to service caller
     *
     * @return
     */
    public static Stream getStream(String taskid, String bufferid, PagesSerde serde)
    {
        log.info("Getting output stream for: " + taskid + "-" + bufferid);
        String key = toKey(taskid, bufferid);
        Stream out = streamMap.get(key);
        if (out == null) {
            out = new Stream(key, serde);
            Stream temp = streamMap.putIfAbsent(key, out);
            out = temp != null ? temp : out;
        }
        return out;
    }

    /**
     * must be used in a the following way to ensure proper handling of releasing the resources
     * try (Out out = ShuffleService.getOutStream(task)) {
     * out.write(page);
     * }
     */
    public static class Stream
            implements AutoCloseable
    {
        static final SerializedPage EOS = new SerializedPage(EMPTY_SLICE, PageCodecMarker.MarkerSet.empty(), 0, 0);
        private final PagesSerde serde;
        ArrayBlockingQueue<SerializedPage> queue = new ArrayBlockingQueue(100 /** shuffle.grpc.buffer_size_in_item */);
        String id;
        boolean eos; // endOfStream

        private Stream(String id, PagesSerde serde)
        {
            this.id = id;
            this.serde = serde;
        }

        SerializedPage take()
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
    }
}
