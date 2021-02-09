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

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import nova.hetu.shuffle.inmemory.LocalShuffleClient;
import nova.hetu.shuffle.ucx.UcxShuffleClient;

import java.io.Closeable;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Objects.requireNonNull;

public class PageConsumer
        implements Closeable
{
    LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
    PagesSerde serde;
    private final AtomicBoolean shuffleClientFinished = new AtomicBoolean();
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private ShuffleClient shuffleClient;

    public static PageConsumer create(ProducerInfo producerInfo, PagesSerde serde)
    {
        return new PageConsumer(producerInfo, serde, PagesSerde.CommunicationMode.STANDARD);
    }

    public static PageConsumer create(ProducerInfo producerInfo, PagesSerde serde, boolean forceCommunication)
    {
        // If we are forcing the communication, does not matter whether we are on the same server or not
        if (forceCommunication) {
            return new PageConsumer(producerInfo, serde, PagesSerde.CommunicationMode.STANDARD);
        }

        // Compare my ip and location ip and if match, initiate in-memory page shuffling
        String myIP = null;
        try {
            final DatagramSocket socket = new DatagramSocket();
            socket.connect(InetAddress.getByName("8.8.8.8"), 10002);
            myIP = socket.getLocalAddress().getHostAddress();
        }
        catch (SocketException e) {
            throw new RuntimeException(e);
        }
        catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        if (producerInfo.getHost().equals("127.0.0.1") || myIP.equals(producerInfo.getHost())) {
            // TODO: in memory is not work right now.
//            return new PageConsumer(producerInfo, serde, PagesSerde.CommunicationMode.INMEMORY);
        }

        return new PageConsumer(producerInfo, serde, PagesSerde.CommunicationMode.STANDARD);
    }

    PageConsumer(ProducerInfo producerInfo, PagesSerde serde, PagesSerde.CommunicationMode commMode)
    {
        this.pageOutputBuffer = new LinkedBlockingQueue<>();
        this.serde = serde;

        switch (commMode) {
            case INMEMORY:
                shuffleClient = new LocalShuffleClient();
                break;
            case STANDARD:
                // TODO: pass in an event listener to handler success and failure events
                shuffleClient = new UcxShuffleClient();
                break;
            default:
                throw new RuntimeException("Unsupported PageConsumer type: " + commMode);
        }
        shuffleClient.getResults(producerInfo.getHost(), producerInfo.getPort(), producerInfo.getProducerId(), pageOutputBuffer, new ShuffleClientCallbackImpl());
    }

    public Page poll()
    {
        Throwable t = failure.get();
        if (t != null) {
            throw new RuntimeException(t);
        }
        SerializedPage page = pageOutputBuffer.poll();
        if (page == null) {
            return null;
        }
        return serde.deserialize(page);
    }

    public boolean isEnded()
    {
        return pageOutputBuffer.isEmpty() && shuffleClientFinished.get();
    }

    @Override
    public void close()
            throws IOException
    {
        shuffleClient.close();
        pageOutputBuffer.clear();
    }

    private class ShuffleClientCallbackImpl
            implements ShuffleClientCallback
    {
        @Override
        public void clientFinished()
        {
            shuffleClientFinished.compareAndSet(false, true);
        }

        @Override
        public void clientFailed(Throwable cause)
        {
            requireNonNull(cause, "cause is null");
            if (!isEnded()) {
                failure.compareAndSet(null, cause);
            }
        }
    }
}
