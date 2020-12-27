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

import com.google.protobuf.ByteString;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import org.apache.log4j.Logger;

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
    private static Logger log = Logger.getLogger(ShuffleService.class);

    public ShuffleService() {}

    /**
     * Down stream operators call this method via gRpc to retrieve the output of the task
     *
     * @param producer
     * @param responseObserver
     */
    @Override
    public void getResult(ShuffleOuterClass.Producer producer, StreamObserver<ShuffleOuterClass.Page> responseObserver)
    {
        final String producerId = producer.getProducerId();
        log.info("====================== Get result for " + producerId);
        ServerCallStreamObserver<ShuffleOuterClass.Page> serverCallStreamObserver = (ServerCallStreamObserver<ShuffleOuterClass.Page>) responseObserver;
        Stream stream = Stream.get(producer.getProducerId());

        /**
         * Wait until stream is created, another way is to simply return and let the client try again
         */
        long maxWait = 1000;
        long sleepInterval = 50;
        while (stream == null && !serverCallStreamObserver.isCancelled() && maxWait > 0) {
            stream = Stream.get(producerId);
            try {
                maxWait -= sleepInterval;
                Thread.sleep(sleepInterval);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (stream != null) {
                log.info("Got output stream after retry " + producerId);
            }
        }

        if (stream == null) {
            throw new RuntimeException("invalid producer: " + producerId);
        }
        int count = 0;
        SerializedPage page;
        try {
            while (!serverCallStreamObserver.isCancelled()) {
                if (stream instanceof BroadcastStream) {
                    int channelId = Integer.parseInt(producerId.split("-")[1]);
                    page = ((BroadcastStream) stream).take(channelId);
                }
                else {
                    page = stream.take();
                }
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
            Stream.destroy(stream);
            responseObserver.onCompleted();
            log.info("====================== Finished sending pages for " + producerId + " count:" + count);
        }
    }

    private static String toKey(String producerId, String bufferid)
    {
        return producerId + "/" + bufferid;
    }

    private ShuffleOuterClass.Page transform(SerializedPage page)
    {
        return ShuffleOuterClass.Page.newBuilder()
                .setSliceArray(ByteString.copyFrom(page.getSliceArray()))
                .setPageCodecMarkers(page.getPageCodecMarkers())
                .setPositionCount(page.getPositionCount())
                .setUncompressedSizeInBytes(page.getUncompressedSizeInBytes())
                .build();
    }
}
