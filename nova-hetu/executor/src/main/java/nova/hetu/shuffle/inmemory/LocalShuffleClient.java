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
package nova.hetu.shuffle.inmemory;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import nova.hetu.shuffle.ShuffleClient;
import nova.hetu.shuffle.ShuffleClientCallback;
import nova.hetu.shuffle.stream.Stream;
import nova.hetu.shuffle.stream.StreamManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static nova.hetu.shuffle.stream.Constants.EOS;

public class LocalShuffleClient
        implements ShuffleClient
{
    private static final ExecutorService executor = Executors.newWorkStealingPool();
    private static final Logger log = Logger.getLogger(LocalShuffleClient.class);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private Future resultFuture;
    private ShuffleClientCallback shuffleClientCallback;

    public void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
    {
        log.info("******************* Local shuffle client for producer " + producerId);
        this.shuffleClientCallback = shuffleClientCallback;
        resultFuture = executor.submit(() -> {
            while (!closed.get()) {
                Stream stream = null;
                while (stream == null && !closed.get()) {
                    stream = StreamManager.getStream(producerId, PagesSerde.CommunicationMode.INMEMORY, StreamManager.DEFAULT_MAX_WAIT, StreamManager.DEFAULT_SLEEP_INTERVAL);
                }
                if (stream == null) {
                    log.warn("Could not get Stream for Producer: " + producerId + " before exchange client closed");
                    return;
                }
                try {
                    SerializedPage page = stream.take();
                    if (page == EOS) {
                        stream.destroy();
                        shuffleClientCallback.clientFinished();
                        log.info("Local shuffle client is done");
                        return;
                    }
                    else {
                        pageOutputBuffer.put(page);
                    }
                    log.info(page);
                }
                catch (Exception e) {
                    shuffleClientCallback.clientFailed(e);
                }
            }
        });
    }

    @Override
    public void close()
            throws IOException
    {
        closed.set(true);
        if (resultFuture != null && !resultFuture.isDone()) {
            resultFuture.cancel(true);
            this.shuffleClientCallback.clientFinished();
        }
    }
}
