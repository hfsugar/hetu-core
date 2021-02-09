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

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.block.Block;
import nova.hetu.omnicache.vector.Vec;
import nova.hetu.shuffle.stream.Stream;
import nova.hetu.shuffle.stream.StreamManager;
import nova.hetu.shuffle.ucx.memory.RegisteredMemory;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import nova.hetu.shuffle.ucx.message.UcxCloseMessage;
import nova.hetu.shuffle.ucx.message.UcxMessage;
import nova.hetu.shuffle.ucx.message.UcxPageMessage;
import nova.hetu.shuffle.ucx.message.UcxSetupMessage;
import nova.hetu.shuffle.ucx.message.UcxTakeMessage;
import org.apache.log4j.Logger;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpEndpoint;
import org.openucx.jucx.ucp.UcpEndpointParams;
import org.openucx.jucx.ucp.UcpMemory;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static nova.hetu.shuffle.stream.Constants.EOS;

public class UcxShuffleService
        implements Runnable, Closeable
{
    private static final Logger log = Logger.getLogger(UcxShuffleService.class);
    private final UcpContext context;
    private final UcpWorker worker;
    private final UcpEndpoint endpoint;
    private final ExecutorService serverExecutor;
    private final ConcurrentMap<Integer, UcxStream> allStreams = new ConcurrentHashMap<>();
    private final ByteBuffer setupMessageBuffer;
    private final UcxMemoryPool ucxMemoryPool;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public UcxShuffleService(UcpContext context, UcxMemoryPool ucxMemoryPool, ByteBuffer setupMessageBuffer, ExecutorService serverExecutor)
    {
        UcxSetupMessage metadata = new UcxSetupMessage(setupMessageBuffer);

        this.worker = context.newWorker(new UcpWorkerParams().requestThreadSafety());
        this.endpoint = worker.newEndpoint(
                new UcpEndpointParams()
                        .setUcpAddress(metadata.getUcpWorkerAddress()));
        this.serverExecutor = serverExecutor;
        this.setupMessageBuffer = setupMessageBuffer;
        this.context = context;
        this.ucxMemoryPool = ucxMemoryPool;
    }

    private void waitForMessage(ByteBuffer messageBuffer)
    {
        messageBuffer.clear();
        // Not great, should replace with a single message and blocking worker.progressRequest
        worker.recvTaggedNonBlocking(messageBuffer, 0, 0, new UcxCallback()
        {
            @Override
            public void onSuccess(UcpRequest request)
            {
                processRequest(messageBuffer);
                super.onSuccess(request);
            }

            @Override
            public void onError(int ucsStatus, String errorMsg)
            {
                log.error("Status: " + ucsStatus + " Error: " + errorMsg);
                super.onError(ucsStatus, errorMsg);
            }
        });
    }

    private void processRequest(ByteBuffer buffer)
    {
        // process request
        UcxMessage.UcxMessageType msgType = UcxMessage.parseType(buffer);
        switch (msgType) {
            case SETUP:
                // setup message, return ucp worker address.
                log.info("Server handle SETUP message.");
                RegisteredMemory setupBuffer = new UcxSetupMessage.Builder(this.ucxMemoryPool)
                        .setUcpWorkerAddress(worker.getAddress())
                        .build();
                endpoint.sendTaggedNonBlocking(setupBuffer.getBuffer(), new UcxRegisteredMemoryCallback(setupBuffer, ucxMemoryPool));
                break;
            case TAKE:
                // queue write request to specific stream
                UcxTakeMessage take = new UcxTakeMessage(buffer);
                UcxStream stream = getOrCreate(take.getProducerId(), take.getId());
                log.info("Server handle " + take);
                stream.enqueue(take.getRateLimit());
                break;
            case CLOSE:
                // close stream
                UcxCloseMessage close = new UcxCloseMessage(buffer);
                log.info("Server handle " + close);
                stream = allStreams.remove(close.getId());
                if (stream != null) {
                    try {
                        stream.close();
                    }
                    catch (IOException e) {
                        log.warn("Server handle CLOSE failed [" + close.getId() + "] [" + close.getProducerId() + "]message.");
                    }
                }
                else {
                    log.warn("Server handle CLOSE failed [" + close.getId() + "] [" + close.getProducerId() + "] - cannot find stream");
                }
                break;
            default:
                log.warn("Unkown Message: " + msgType);
                break;
        }
        waitForMessage(buffer);
    }

    private synchronized UcxStream getOrCreate(String producerId, int id)
    {
        UcxStream ucxStream = allStreams.get(id);
        if (ucxStream != null) {
            return ucxStream;
        }
        ucxStream = new UcxStream(producerId, id);
        allStreams.put(id, ucxStream);
        return ucxStream;
    }

    @Override
    public void run()
    {
        processRequest(setupMessageBuffer);
        while (!closed.get()) {
            if (worker.progress() == 0) {
                worker.waitForEvents();
            }
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed.set(false);
        allStreams.forEach((k, v) -> {
            try {
                v.close();
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        });
        endpoint.closeNonBlockingFlush();
        worker.close();
        allStreams.clear();
    }

    private class UcxStream
            implements Closeable
    {
        private final String producerId;
        private final int id;
        private final AtomicInteger queued = new AtomicInteger(0);
        private final Stack<Closeable> resources = new Stack<>();
        private Stream stream;
        private int sendId;

        public UcxStream(String producerId, int id)
        {
            this.producerId = producerId;
            this.id = id;
            this.stream = StreamManager.get(producerId, PagesSerde.CommunicationMode.UCX);
            this.sendId = 0;
        }

        public void enqueue(int nbRequests)
        {
            if (stream != null && stream.isClosed()) {
                return;
            }
            int beforeAdd = queued.getAndAdd(nbRequests);
            //TODO manage requests and pre allocate them here
            if (beforeAdd == 0) {
                serverExecutor.submit(this::process);
            }
        }

        private void getStream()
        {
            if (stream == null) {
                long maxWait = 10000;
                long sleepInterval = 50;
                while (stream == null && maxWait > 0) {
                    stream = StreamManager.get(producerId, PagesSerde.CommunicationMode.UCX);
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
            }
            if (stream == null) {
                log.error("Couldn't get the stream: " + producerId);
                //FIXME we need to clean up the stream entry
            }
        }

        private long getTag()
        {
            long tmp = id;
            tmp = tmp << 32;
            tmp = tmp + sendId++;
            return tmp;
        }

        private void sendEos(long tag)
        {
            UcxPageMessage.Builder builder = new UcxPageMessage.Builder(ucxMemoryPool);
            RegisteredMemory buffer = builder
                    .setUncompressedSizeInBytes(0)
                    .build();
            log.trace("Server send EOS: [" + tag + "] [" + id + "] " + builder.toString());
            endpoint.sendTaggedNonBlocking(buffer.getBuffer(), tag, new UcxCallback()
            {
                @Override
                public void onSuccess(UcpRequest request)
                {
                    ucxMemoryPool.put(buffer);
                    super.onSuccess(request);
                }

                @Override
                public void onError(int ucsStatus, String errorMsg)
                {
                    ucxMemoryPool.put(buffer);
                    super.onError(ucsStatus, errorMsg);
                }
            });
        }

        private RegisteredMemory onHeapMemory(long tag, SerializedPage page)
        {
            byte[] slice = page.getSliceArray();
            ByteBuffer pageBuffer = ByteBuffer.allocateDirect(slice.length);
            // will free by client when client UcpRemoteKey.close()
            UcpMemory pageMemory = context.registerMemory(pageBuffer);
            pageBuffer.put(slice);
            pageBuffer.clear();
            resources.push(pageMemory);

            UcxPageMessage.BlockMetadata blockMetadata = new UcxPageMessage.BlockMetadata(
                    pageMemory.getAddress(),
                    pageMemory.getLength(),
                    page.getPositionCount(),
                    pageMemory.getRemoteKeyBuffer(),
                    pageBuffer.hashCode());

            UcxPageMessage.Builder builder = new UcxPageMessage.Builder(ucxMemoryPool)
                    .addBlockMetadata(blockMetadata)
                    .setOffHeap(false)
                    .setPageCodecMarkers(page.getPageCodecMarkers())
                    .setPositionCount(page.getPositionCount())
                    .setUncompressedSizeInBytes(page.getUncompressedSizeInBytes());

            log.trace("Server send on heap page: [" + tag + "]" + builder.toString());

            return builder.build();
        }

        private RegisteredMemory offHeapMemory(long tag, SerializedPage page)
        {
            UcxPageMessage.Builder builder = new UcxPageMessage.Builder(ucxMemoryPool)
                    .setOffHeap(true)
                    .setPageCodecMarkers(page.getPageCodecMarkers())
                    .setPositionCount(page.getPositionCount())
                    .setUncompressedSizeInBytes(page.getUncompressedSizeInBytes());

            Block[] blocks = page.getBlocks();
            for (int blockId = 0; blockId < blocks.length; blockId++) {
                Vec vec = blocks[blockId].getVec();
                ByteBuffer blockBuffer = vec.getData();
                // will free by client when client UcpRemoteKey.close()
                UcpMemory blockMemory = context.registerMemory(blockBuffer);
                resources.push(blockMemory);

                UcxPageMessage.BlockMetadata blockMetadata = new UcxPageMessage.BlockMetadata(
                        blockMemory.getAddress(),
                        blockMemory.getLength(),
                        blocks[blockId].getPositionCount(),
                        blockMemory.getRemoteKeyBuffer(),
                        blockBuffer.hashCode());

                builder.addBlockMetadata(blockMetadata);
            }

            log.trace("Server send off heap page: [" + tag + "]" + builder.toString());

            return builder.build();
        }

        private void sendPage(SerializedPage page, long tag)
        {
            // we register the memory and send the msg
            RegisteredMemory buffer;
            if (page.isOffHeap()) {
                buffer = offHeapMemory(tag, page);
            }
            else {
                buffer = onHeapMemory(tag, page);
            }

            endpoint.sendTaggedNonBlocking(buffer.getBuffer(), tag, new UcxCallback()
            {
                @Override
                public void onSuccess(UcpRequest request)
                {
                    ucxMemoryPool.put(buffer);
                    super.onSuccess(request);
                }

                @Override
                public void onError(int ucsStatus, String errorMsg)
                {
                    ucxMemoryPool.put(buffer);
                    super.onError(ucsStatus, errorMsg);
                }
            });
        }

        public void resetResource()
        {
            while (!resources.empty()) {
                try {
                    resources.pop().close();
                }
                catch (IOException e) {
                    log.warn("release resource failed. " + e);
                }
            }
        }

        public void process()
        {
            resetResource();

            getStream();

            boolean isEOS = false;
            //FIXME if stream is null, send exeption back ...
            if (stream == null || (stream != null && stream.isClosed())) {
                isEOS = true;
            }
            while (queued.getAndUpdate(op -> op == 0 ? 0 : op - 1) > 0) {
                try {
                    long tag = getTag();
                    if (isEOS) {
                        sendEos(tag);
                        continue;
                    }
                    SerializedPage page = stream.take();
                    if (page == EOS) {
                        sendEos(tag);
                        isEOS = true;
                        stream.destroy();
                    }
                    else {
                        sendPage(page, tag);
                    }
                }
                catch (Exception e) {
                    log.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void close()
                throws IOException
        {
            resetResource();
        }
    }
}
