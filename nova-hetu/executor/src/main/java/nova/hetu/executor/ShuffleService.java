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
package nova.hetu.executor;

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import nova.hetu.executor.PageProducer.Type;
import org.apache.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;

/**
 * ####Shuffle Service
 * Shuffle service is responsible shuffling data between tasks. It should be easy for the user of Shuffle service to set up
 * communication between tasks based on the DAG of stages.
 * <p>
 * ######Requirements
 * 1. A single point responsible for communication between tasks
 * 2. Capable of `1 to 1`, `1 to M`, `M to 1` and `M to M` communication paradigms
 * 3. Transparent back pressure
 * 4. Non-blocking
 * <p>
 * This is designed to allow easy reimplementation over other prototcal such as RSocket: https://github.com/rsocket/rsocket-java
 */
public class ShuffleService
        extends ShuffleGrpc.ShuffleImplBase
{
    private static ConcurrentHashMap<String, Stream> streamMap = new ConcurrentHashMap<>();

    private static Logger log = Logger.getLogger(ShuffleService.class);

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

    private static String toKey(String producerId, String bufferid)
    {
        return producerId + "/" + bufferid;
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
     * Returns a OutStream which will be used PRODUCER to sent the data to be returned to service caller
     *
     * @return
     */
    static Stream getStream(String producerId, PagesSerde serde, Type type)
    {
        log.info("Getting output stream for: " + producerId);
        Stream out = streamMap.get(producerId);
        if (out == null) {
            out = new StreamFactory().create(producerId, serde, type);
            Stream temp = streamMap.putIfAbsent(producerId, out);
            out = temp != null ? temp : out;
        }
        return out;
    }
}
