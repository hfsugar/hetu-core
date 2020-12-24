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

import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.rsocket.ConnectionSetupPayload;
import io.rsocket.Payload;
import io.rsocket.RSocket;
import io.rsocket.SocketAcceptor;
import io.rsocket.util.DefaultPayload;
import nova.hetu.shuffle.Stream;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class PageHandler
        implements SocketAcceptor
{
    @Override
    public Mono<RSocket> accept(ConnectionSetupPayload connectionSetupPayload, RSocket rSocket)
    {
        return Mono.just(new RSocket()
        {
            @Override
            public Flux<Payload> requestStream(Payload payload)
            {
                String producerid = payload.getDataUtf8();
                System.out.println("requesting stream: " + producerid);
                Stream stream = Stream.get(producerid);
                return getFlux_Sink(stream);
            }
        });
    }

    private Flux<Payload> getFlux_Sink(Stream stream)
    {
        return Flux.<Payload>create(sink -> {
            sink.onRequest(n -> {
                for (int i = 0; i < n; i++) {
                    try {
                        SerializedPage page = stream.take();
                        if (page != Stream.EOS) {
                            System.out.println("sending: " + page);
                            sink.next(DefaultPayload.create(page.getSliceArray(), extractMetadata(page)));
                        }
                        else {
                            sink.complete();
                            break;
                        }
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        })
                .subscribeOn(Schedulers.immediate(), true)
                .doOnRequest(value -> System.out.println("requested: " + value))
                .doOnComplete(() -> System.out.println("completing the request"));
    }

    private byte[] extractMetadata(SerializedPage page)
    {
        byte marker = page.getPageCodecMarkers();
        int count = page.getPositionCount();
        int size = page.getUncompressedSizeInBytes();

        return new byte[] {
                marker,
                (byte) (count >>> 24),
                (byte) (count >>> 16),
                (byte) (count >>> 8),
                (byte) count,
                (byte) (size >>> 24),
                (byte) (size >>> 16),
                (byte) (size >>> 8),
                (byte) size};
    }
}
