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

import java.util.concurrent.LinkedBlockingQueue;

import static nova.hetu.shuffle.stream.Constants.EOS;

public class LocalShuffleClient
        implements ShuffleClient
{
    private static Logger log = Logger.getLogger(LocalShuffleClient.class);

    public void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
    {
        log.info("******************* Local shuffle client for producer " + producerId);

        final Thread thread = new Thread(() -> {
            Stream stream = StreamManager.get(producerId, PagesSerde.CommunicationMode.INMEMORY);
            long maxWait = 1000;
            long sleepInterval = 50;
            while (stream == null && maxWait > 0) {
                stream = StreamManager.get(producerId, PagesSerde.CommunicationMode.INMEMORY);
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
                shuffleClientCallback.clientFailed(new RuntimeException("invalid producer: " + producerId));
                return;
            }

            while (stream != null) {
                try {
                    SerializedPage page = stream.take();
                    if (page == EOS) {
                        try {
                            stream.destroy();
                            shuffleClientCallback.clientFinished();
                            log.info("Local shuffle client is done");
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                        break;
                    }
                    try {
                        pageOutputBuffer.put(page);
                    }
                    catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    log.info(page);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        thread.start();
    }
}
