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

import com.google.common.util.concurrent.SettableFuture;
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
import org.apache.log4j.Logger;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

public class RsShuffleClient
{
    private static Logger log = Logger.getLogger(RsShuffleClient.class);
    
    private RsShuffleClient() {}

    public static Future getResults(String host, int port, String producerId, LinkedBlockingQueue<SerializedPage> pageOutputBuffer)
    {
        SettableFuture future = SettableFuture.create();
        RSocket client = RSocketConnector.create()
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .connect(TcpClientTransport.create(host, port))
                .block();

        log.info("*******************Creating flux for result " + producerId);
        Flux<Payload> flux = client.<Payload>requestStream(DefaultPayload.create(producerId.getBytes()))
                .limitRate(1000) //dynamically calculate rate??
                .doOnComplete(() -> {
                    log.info("*******************Closing flux for result " + producerId);
                    future.set(true);
                });

        flux.subscribe(payload -> {
            ByteBuffer metadata = payload.getMetadata();
            byte marker = metadata.get();
            int count = metadata.getInt();
            int size = metadata.getInt();
            Slice slice = Slices.wrappedBuffer(payload.getData());
            SerializedPage page = new SerializedPage(slice, PageCodecMarker.MarkerSet.fromByteValue(marker), count, size);
            log.info("*******************getting page: " + page);
            try {
                pageOutputBuffer.put(page);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            log.info(page);
        });

        return future;
    }

    public static void main(String[] args)
            throws InterruptedException, ExecutionException
    {
        Future future = getResults("127.0.0.1", 7878, "producerId", new LinkedBlockingQueue<SerializedPage>());
        future.get();
    }
}
