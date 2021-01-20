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
import nova.hetu.shuffle.ucx.memory.RegisteredMemory;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import nova.hetu.shuffle.ucx.message.UcxCloseMessage;
import nova.hetu.shuffle.ucx.message.UcxPageMessage;
import nova.hetu.shuffle.ucx.message.UcxSetupMessage;
import nova.hetu.shuffle.ucx.message.UcxTakeMessage;
import org.apache.log4j.Logger;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRemoteKey;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static nova.hetu.shuffle.ucx.message.UcxMessage.MAX_MESSAGE_SIZE;

public class UcxConnection
        implements Closeable
{
    private static final Logger log = Logger.getLogger(UcxConnection.class);
    private final InetSocketAddress address;
    private final UcxConnectionFactory factory;
    private final UcpContext context;
    private final UcpWorker worker;
    private SettableFuture<Boolean> connected;
    private UcpEndpoint endpoint;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicBoolean startedListening = new AtomicBoolean(false);

    public UcxConnection(UcpContext context, InetSocketAddress address, UcxConnectionFactory factory)
    {
        this.address = address;
        this.factory = factory;
        this.context = context;
        this.worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        this.endpoint = worker.newEndpoint(new UcpEndpointParams().setSocketAddress(address));
    }

    public String getHost()
    {
        return this.address.getHostName();
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public void close()
            throws IOException
    {
        this.closed.set(true);
        this.endpoint.close();
        this.worker.close();
        this.factory.closeConnection(this);
    }

    public void connect()
    {
        RegisteredMemory sendBuffer = new UcxSetupMessage.Builder(this.getMemoryPool())
                .setUcpWorkerAddress(worker.getAddress())
                .build();
        endpoint.sendTaggedNonBlocking(sendBuffer.getBuffer(), new UcxRegisteredMemoryCallback(sendBuffer, getMemoryPool()));

        RegisteredMemory recvBuffer = getMemoryPool().get(MAX_MESSAGE_SIZE);
        connected.addListener(() -> {
            if (!startedListening.getAndSet(true)) {
                while (!closed.get()) {
                    if (worker.progress() == 0) {
                        worker.waitForEvents();
                    }
                }
            }
        }, factory.getExecutor());
        UcpRequest request = worker.recvTaggedNonBlocking(recvBuffer.getBuffer(), new UcxCallback()
        {
            @Override
            public void onSuccess(UcpRequest request)
            {
                UcxSetupMessage message = new UcxSetupMessage(recvBuffer.getBuffer());
                endpoint.close();
                endpoint = worker.newEndpoint(new UcpEndpointParams().setUcpAddress(message.getUcpWorkerAddress()));
                connected.set(true);
                getMemoryPool().put(recvBuffer);
                log.info("Client connected to server success.");
                super.onSuccess(request);
            }

            @Override
            public void onError(int ucsStatus, String errorMsg)
            {
                connected.set(false);
                getMemoryPool().put(recvBuffer);
                log.error("Client connected to server failed.");
                super.onError(ucsStatus, errorMsg);
            }
        });
        worker.progressRequest(request);
    }

    public void waitConnected()
            throws Exception
    {
        boolean unconnected = false;
        synchronized (this) {
            if (connected == null || (connected.isDone() && !connected.get())) {
                connected = SettableFuture.create();
                unconnected = true;
            }
        }
        if (unconnected) {
            connect();
        }
        else {
            connected.get();
        }
    }

    public Queue<SettableFuture<SerializedPage>> requestNbPages(String producerId, int id, int msgId, int rateLimit)
    {
        Queue<SettableFuture<SerializedPage>> futurePages = new LinkedList<>();
        for (int i = 0; i < rateLimit; i++) {
            SettableFuture<SerializedPage> pageFuture = queuePageRequest(id, msgId + i);
            futurePages.add(pageFuture);
        }
        requestPages(producerId, id, rateLimit);
        return futurePages;
    }

    private UcxMemoryPool getMemoryPool()
    {
        return this.factory.getMemoryPool();
    }

    private SettableFuture<SerializedPage> queuePageRequest(int id, int i)
    {
        SettableFuture<SerializedPage> future = SettableFuture.create();
        long tmp = id;
        final long tag = (tmp << 32) + i;
        RegisteredMemory pageBuffer = getMemoryPool().get(MAX_MESSAGE_SIZE);
        worker.recvTaggedNonBlocking(pageBuffer.getBuffer(), tag, -1L, new UcxCallback()
        {
            @Override
            public void onSuccess(UcpRequest request)
            {
                UcxPageMessage message = new UcxPageMessage(pageBuffer.getBuffer());
                int uncompressedSizeInBytes = message.getUncompressedSizeInBytes();
                if (uncompressedSizeInBytes == 0) {
                    //EoS
                    log.trace("Client receive EOS: [" + tag + "] " + message.toString());
                    future.set(null);
                    getMemoryPool().put(pageBuffer);
                    super.onSuccess(request);
                    return;
                }

                // TODO: block
                UcxPageMessage.BlockMetadata blockMetadata = message.getBlockMetadata(0);

                long remoteAddress = blockMetadata.getDataAddress();
                long remoteSize = blockMetadata.getDataSize();
                UcpRemoteKey remoteKey = endpoint.unpackRemoteKey(blockMetadata.getDataRkey());

                // Limited to 2GB direct buffer grrrrr...
                // TODO check if using a buffer pool  or if we could allocate a fix size
                ByteBuffer serialisedPage = ByteBuffer.allocateDirect((int) remoteSize);
                UcpMemory serializedPageMemory = context.registerMemory(serialisedPage);
                endpoint.getNonBlocking(remoteAddress, remoteKey, serialisedPage, new UcxCallback()
                {
                    @Override
                    public void onSuccess(UcpRequest request)
                    {
                        // TODO: can not copy buffer from off-heap here, if SerializedPage is a OmniVector array
                        byte[] bytes = new byte[serialisedPage.capacity()];
                        serialisedPage.get(bytes);
                        log.trace("Client read page: [" + tag + "] " + message.toString() + " hashCode: " + serialisedPage.clear().hashCode());
                        // TODO: check hash code.
                        SerializedPage page = new SerializedPage(bytes, message.getPageCodecMarkers(), message.getPositionCount(), uncompressedSizeInBytes);
                        future.set(page);
                        serializedPageMemory.close();
                        // free remote data buffer.
                        remoteKey.close();
                        getMemoryPool().put(pageBuffer);
                        super.onSuccess(request);
                    }

                    @Override
                    public void onError(int ucsStatus, String errorMsg)
                    {
                        future.setException(null);
                        serializedPageMemory.close();
                        // free remote data buffer.
                        remoteKey.close();
                        getMemoryPool().put(pageBuffer);
                        super.onError(ucsStatus, errorMsg);
                    }
                });
                super.onSuccess(request);
            }

            @Override
            public void onError(int ucsStatus, String errorMsg)
            {
                future.setException(null);
                getMemoryPool().put(pageBuffer);
                super.onError(ucsStatus, errorMsg);
            }
        });
        return future;
    }

    private void requestPages(String producerId, int id, int rateLimit)
    {
        RegisteredMemory buffer = new UcxTakeMessage.Builder(this.getMemoryPool())
                .setProducerId(producerId)
                .setId(id)
                .setRateLimit(rateLimit)
                .build();

        endpoint.sendTaggedNonBlocking(buffer.getBuffer(), new UcxCallback()
        {
            @Override
            public void onSuccess(UcpRequest request)
            {
                getMemoryPool().put(buffer);
                super.onSuccess(request);
            }

            @Override
            public void onError(int ucsStatus, String errorMsg)
            {
                getMemoryPool().put(buffer);
                super.onError(ucsStatus, errorMsg);
            }
        });
    }

    public void sendDone(String producerId, int id)
    {
        RegisteredMemory buffer = new UcxCloseMessage.Builder(this.getMemoryPool())
                .setProducerId(producerId)
                .setId(id)
                .build();

        endpoint.sendTaggedNonBlocking(buffer.getBuffer(), new UcxCallback()
        {
            @Override
            public void onSuccess(UcpRequest request)
            {
                getMemoryPool().put(buffer);
                super.onSuccess(request);
            }

            @Override
            public void onError(int ucsStatus, String errorMsg)
            {
                getMemoryPool().put(buffer);
                super.onError(ucsStatus, errorMsg);
            }
        });
    }
}
