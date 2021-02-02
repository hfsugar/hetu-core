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
package nova.hetu.shuffle.ucx;

import com.google.common.util.concurrent.SettableFuture;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import nova.hetu.shuffle.ShuffleClient;
import nova.hetu.shuffle.ShuffleClientCallback;
import org.apache.log4j.Logger;

import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class UcxShuffleClient
        implements ShuffleClient
{
    private static final Logger log = Logger.getLogger(UcxShuffleClient.class);
    private static final UcxConnectionFactory factory = new UcxConnectionFactory();
    private static final ExecutorService executor = Executors.newWorkStealingPool();
    private static final AtomicInteger idCounter = new AtomicInteger(0);

    public UcxShuffleClient() {}

    public void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback callback)
    {
        UcxConnection connection = factory.getOrCreateConnection(host, port);

        // FIXME ratelimit is hardcoded to match rsclient .. but we need to make it variable
        Fetch fetch = new Fetch(pageOutputBuffer, connection, producerId, UcxConstant.DEFAULT_RATE_LIMIT, callback);
        executor.submit(fetch::getPages);
    }

    private static class Fetch
    {
        private final UcxConnection connection;
        private final String producerId;
        private final ShuffleClientCallback callback;
        private final int id;
        private final LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
        private final int rateLimit;
        private int msgId;

        public Fetch(LinkedBlockingQueue<SerializedPage> pageOutputBuffer, UcxConnection connection, String producerId, int rateLimit, ShuffleClientCallback callback)
        {
            this.msgId = 0;
            this.connection = connection;
            this.producerId = producerId;
            this.callback = callback;
            this.id = idCounter.addAndGet(1);
            this.pageOutputBuffer = pageOutputBuffer;
            this.rateLimit = rateLimit;
        }

        public void getPages()
        {
            log.info(producerId + " START GET PAGES.");
            try {
                connection.waitConnected();
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }

            boolean isDone = false;
            while (!isDone) {
                Queue<SettableFuture<SerializedPage>> futureQueue = connection.requestNbPages(producerId, id, msgId, rateLimit);
                this.msgId = this.msgId + rateLimit;
                while (!futureQueue.isEmpty()) {
                    SettableFuture<SerializedPage> pageFuture = futureQueue.remove();
                    try {
                        SerializedPage page = pageFuture.get();
                        // TODO handle exception ...
                        if (page != null) {
                            // TODO : monitor the page output buffer to adapt the ratelimit
                            pageOutputBuffer.put(page);
                        }
                        else if (!isDone) {
                            // Got EoS
                            isDone = true;
                        }
                    }
                    catch (InterruptedException | ExecutionException e) {
                        callback.clientFailed(e);
                        isDone = true;
                    }
                }
            }
            connection.sendDone(producerId, id);
            callback.clientFinished();
            log.info(producerId + " GET PAGES DONE.");
        }
    }
}
