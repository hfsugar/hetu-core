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

import com.google.common.util.concurrent.SettableFuture;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import nova.hetu.executor.ExecutorOuterClass;
import nova.hetu.executor.ShuffleGrpc;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class ShuffleClient
{
    public static ExecutorService executor = Executors.newFixedThreadPool(10);
    public static ManagedChannel channel;

    private static Logger log = Logger.getLogger(ShuffleClient.class);

    private ShuffleClient() {}

    private static synchronized ManagedChannel getOrCreateChannel(String host, int port)
    {
        if (channel == null) {
            channel = ManagedChannelBuilder
                    .forAddress(host, port)
                    .usePlaintext(/** FIXME: TLS disabled */)
                    .build();
        }
        return channel;
    }

    /**
     * Get the execution results identified by a taskid and bufferid
     * The method runs in a separate thread. The SerializedPage is streamed back, streaming of pages terminates when there is not futher pages.
     *
     * @param host the host the task is running
     * @param port the port the task is listening
     * @param taskid identifier of the task
     * @param bufferid identifier of the partition of the task to be retrieved
     * @return
     */
    public static Future getResults(String host, int port, String taskid, String bufferid, LinkedBlockingQueue<SerializedPage> pageOutputBuffer)
    {
        ManagedChannel channel = getOrCreateChannel(host, port);

        SettableFuture future = SettableFuture.create();

        ShuffleGrpc.ShuffleStub shuffler = ShuffleGrpc.newStub(channel);

        ExecutorOuterClass.Task task = ExecutorOuterClass.Task.newBuilder()
                .setTaskId(taskid)
                .setBufferId(bufferid)
                .build();
        log.info("====================== request " + task.getTaskId() + "-" + task.getBufferId());

        /**
         * get the result in a new thread, which should add the result into the pages buffer
         */
        shuffler.getResult(task, new ShuffleStreamObserver(pageOutputBuffer, future));
        return future;
    }

    private static class ShuffleStreamObserver
            implements StreamObserver<ExecutorOuterClass.Page>
    {
        LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
        SettableFuture<Boolean> notificationFuture;
        int count;

        ShuffleStreamObserver(LinkedBlockingQueue<SerializedPage> pageOutputBuffer, SettableFuture notificationFuture)
        {
            this.pageOutputBuffer = pageOutputBuffer;
            this.notificationFuture = notificationFuture;
        }

        @Override
        public void onNext(ExecutorOuterClass.Page page)
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
            notificationFuture.set(true);
        }
    }
}
