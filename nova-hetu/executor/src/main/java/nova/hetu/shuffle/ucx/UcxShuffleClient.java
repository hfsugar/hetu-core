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
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static nova.hetu.shuffle.stream.Constants.EOS;

public class UcxShuffleClient
        implements ShuffleClient
{
    private static final Logger log = Logger.getLogger(UcxShuffleClient.class);
    private static final UcxConnectionFactory factory = new UcxConnectionFactory();
    private static final ExecutorService executor = Executors.newCachedThreadPool();
    private static final AtomicInteger idCounter = new AtomicInteger(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public UcxShuffleClient() {}

    @Override
    public void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
    {
        UcxConnection connection = factory.getOrCreateConnection(host, port);

        Fetch fetch = new Fetch(pageOutputBuffer, connection, producerId, shuffleClientCallback, closed);
        executor.submit(fetch::getPages);
    }

    @Override
    public void close()
            throws IOException
    {
        closed.set(true);
    }

    private static class Fetch
    {
        private final UcxConnection connection;
        private final String producerId;
        private final ShuffleClientCallback shuffleClientCallback;
        private final int id;
        private final LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
        private final AtomicBoolean closed;
        private int rateLimit;
        private int msgId;
        private int numPagesBeforePrefetch;

        public Fetch(LinkedBlockingQueue<SerializedPage> pageOutputBuffer, UcxConnection connection, String producerId, ShuffleClientCallback shuffleClientCallback, AtomicBoolean closed)
        {
            this.msgId = 0;
            this.connection = connection;
            this.producerId = producerId;
            this.shuffleClientCallback = shuffleClientCallback;
            this.id = idCounter.addAndGet(1);
            this.pageOutputBuffer = pageOutputBuffer;
            this.rateLimit = shuffleClientCallback.updateRateLimit();
            this.numPagesBeforePrefetch = (int) (rateLimit * UcxConstant.DEFAULT_PREFETCH_COEFF);
            this.closed = closed;
        }

        public void getPages()
        {
            log.info(producerId + " START GET PAGES.");
            try {
                connection.waitConnected();
            }
            catch (Exception e) {
                shuffleClientCallback.clientFailed(e);
                return;
            }
            UcxMemoryPool ucxPageMemoryPool = new UcxMemoryPool(connection.getContext(), DEFAULT_MAX_PAGE_SIZE_IN_BYTES);
            ucxPageMemoryPool.preAlocate((int) Math.ceil(UcxConstant.DEFAULT_RATE_LIMIT / 4), UcxConstant.UCX_MIN_BUFFER_SIZE);
            SettableFuture<SerializedPage> pendingFuture = null;
            LinkedBlockingQueue<SettableFuture<SerializedPage>> futureQueue = new LinkedBlockingQueue<>();
            int numProcessedPages = 0;
            connection.requestNbPages(futureQueue, ucxPageMemoryPool, producerId, id, msgId, rateLimit, 0);
            int lastRateLimit = rateLimit;
            this.msgId = this.msgId + rateLimit;
            while (!futureQueue.isEmpty() || !closed.get()) {
                if (numProcessedPages == numPagesBeforePrefetch) {
                    lastRateLimit = rateLimit;
                    rateLimit = shuffleClientCallback.updateRateLimit();
                    if (rateLimit != lastRateLimit) {
                        numPagesBeforePrefetch = (int) (rateLimit * UcxConstant.DEFAULT_PREFETCH_COEFF);
                    }

                    // Basically put on hold while the rate limit is not increased
                    // Maybe too aggressive
                    while (rateLimit == 0) {
                        try {
                            Thread.sleep(50);
                        }
                        catch (InterruptedException e) {
                            shuffleClientCallback.clientFailed(e);
                            break;
                        }
                        rateLimit = shuffleClientCallback.updateRateLimit();
                    }
                    connection.requestNbPages(futureQueue, ucxPageMemoryPool, producerId, id, msgId, rateLimit, numPagesBeforePrefetch);
                    this.msgId = this.msgId + rateLimit;
                }
                SettableFuture<SerializedPage> pageFuture = futureQueue.remove();
                numProcessedPages = (numProcessedPages + 1) % lastRateLimit;
                try {
                    SerializedPage page = null;
                    while (page == null && !closed.get()) {
                        try {
                            page = pageFuture.get(5, TimeUnit.SECONDS);
                        }
                        catch (TimeoutException ignored) {
                        }
                    }
                    if (page == null) {
                        pendingFuture = pageFuture;
                        shuffleClientCallback.clientFinished();
                        break;
                    }
                    if (page != EOS) {
                        // TODO : monitor the page output buffer to adapt the ratelimit
                        pageOutputBuffer.put(page);
                    }
                    else {
                        // Got EoS
                        shuffleClientCallback.clientFinished();
                        break;
                    }
                }
                catch (InterruptedException | ExecutionException e) {
                    shuffleClientCallback.clientFailed(e);
                    break;
                }
            }
            connection.sendDone(producerId, id);
            while (!futureQueue.isEmpty()) {
                SettableFuture<SerializedPage> pageFuture = null;
                if (pendingFuture == null) {
                    pageFuture = futureQueue.remove();
                }
                else {
                    pageFuture = pendingFuture;
                    pendingFuture = null;
                }
                try {
                    pageFuture.get();
                }
                catch (InterruptedException | ExecutionException e) {
                    log.warn("Error cleaning up EoS message " + e);
                }
            }
            ucxPageMemoryPool.close();
            log.info(producerId + " GET PAGES DONE.");
        }
    }
}
