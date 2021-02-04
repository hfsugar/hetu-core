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

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class GrpcShuffleClient
        implements ShuffleClient
{
    public static ExecutorService executor = Executors.newFixedThreadPool(10);
    //WE NEED TO CACHE THE CHANNLES, ONE CHANNLE FOR EACH TARGET

    public static HashMap<String, ManagedChannel> channels = new HashMap<>(); //key = "host - port"

    private static Logger log = Logger.getLogger(GrpcShuffleClient.class);

    private GrpcShuffleClient() {}

    private static synchronized ManagedChannel getOrCreateChannel(String host, int port)
    {
        String key = host + "-" + port;
        ManagedChannel channel = channels.get(key);
        if (channel == null) {
            channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext(/** FIXME: TLS disabled */)
                    .build();
            channels.putIfAbsent(key, channel);
        }
        return channel;
    }

    /**
     * Get the execution results identified by a taskid and bufferid
     * The method runs in a separate thread. The SerializedPage is streamed back, streaming of pages terminates when there is not futher pages.
     *
     * @param host the host the task is running
     * @param port the port the task is listening
     * @param producerId identifier of the task
     * @return
     */
    public void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
    {
        ManagedChannel channel = getOrCreateChannel(host, port);

        ShuffleGrpc.ShuffleStub shuffler = ShuffleGrpc.newStub(channel);

        ShuffleOuterClass.Producer task = ShuffleOuterClass.Producer.newBuilder()
                .setProducerId(producerId)
                .build();
        log.info("====================== request " + producerId);

        /**
         * get the result in a new thread, which should add the result into the pages buffer
         */
        shuffler.getResult(task, new ShuffleStreamObserver(pageOutputBuffer, shuffleClientCallback));
    }

    private static class ShuffleStreamObserver
            implements StreamObserver<ShuffleOuterClass.Page>
    {
        LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
        ShuffleClientCallback shuffleClientCallback;
        int count;

        ShuffleStreamObserver(LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
        {
            this.pageOutputBuffer = pageOutputBuffer;
            this.shuffleClientCallback = shuffleClientCallback;
        }

        @Override
        public void onNext(ShuffleOuterClass.Page page)
        {
            count++;
            SerializedPage spage = new SerializedPage(page.getSliceArray().toByteArray(), (byte) page.getPageCodecMarkers(), page.getPositionCount(), page.getUncompressedSizeInBytes());
            try {
                pageOutputBuffer.put(spage);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            catch (Exception e) {
                log.error(e.getMessage());
            }
        }

        @Override
        public void onError(Throwable throwable)
        {
            throw new RuntimeException(throwable);
        }

        @Override
        public void onCompleted()
        {
            shuffleClientCallback.clientFinished();
        }
    }
}
