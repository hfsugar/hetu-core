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
package nova.hetu.shuffle.rsocket;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.core.RSocketConnector;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.client.TcpClientTransport;
import io.rsocket.util.DefaultPayload;
import nova.hetu.shuffle.ShuffleClientCallback;
import org.apache.log4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

public class RsShuffleClient
{
    private static Logger log = Logger.getLogger(RsShuffleClient.class);

    private RsShuffleClient() {}

    public static void getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer, ShuffleClientCallback shuffleClientCallback)
    {
        RSocket client = RSocketConnector.create()
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .connect(TcpClientTransport.create(host, port))
                .cache()
                .block();

        log.info("*******************Creating flux for result " + producerId);
        Flux<Payload> flux = client.<Payload>requestStream(DefaultPayload.create(producerId.getBytes()))
                .limitRate(1000) //dynamically calculate rate??
                .doOnComplete(() -> {
                    log.info("*******************Closing flux for result " + producerId);
                    shuffleClientCallback.clientFinished();
                })
                .doOnCancel(() -> {
                    log.info(producerId + " CANCELLED");
                    shuffleClientCallback.clientFinished();
                })
                .doOnError(e -> {
                    String error = producerId + " Error getting pages: " + e.getMessage();
                    shuffleClientCallback.clientFailed(new Throwable(error));
                });

        flux.subscribe(payload -> {
            ByteBuffer metadata = payload.getMetadata();
            byte marker = metadata.get();
            int count = metadata.getInt();
            int size = metadata.getInt();

            SerializedPage page;
            try {
                Slice slice = Slices.wrappedBuffer(payload.getData());
                page = new SerializedPage(slice, PageCodecMarker.MarkerSet.fromByteValue(marker), count, size);
            }
            catch (Exception e) {
                log.error(producerId + "Error receiving page for " +
                        " with marker: " + marker + ", count " + count +
                        ", size " + size + ": " + e.getMessage());
                return;
            }

            try {
                pageOutputBuffer.put(page);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static void main(String[] args)
            throws InterruptedException, ExecutionException
    {
        getResults("127.0.0.1", 7878, "producerId", new LinkedBlockingQueue<>(), null);
    }
}
