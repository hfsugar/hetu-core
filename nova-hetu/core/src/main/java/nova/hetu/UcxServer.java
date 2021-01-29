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
package nova.hetu;

import nova.hetu.shuffle.ucx.UcxConstant;
import nova.hetu.shuffle.ucx.UcxShuffleService;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import org.apache.log4j.Logger;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpListener;
import org.openucx.jucx.ucp.UcpListenerParams;
import org.openucx.jucx.ucp.UcpParams;
import org.openucx.jucx.ucp.UcpRequest;
import org.openucx.jucx.ucp.UcpWorker;
import org.openucx.jucx.ucp.UcpWorkerParams;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static nova.hetu.shuffle.ucx.message.UcxSetupMessage.MAX_MESSAGE_SIZE;

public class UcxServer
{
    private static final ExecutorService listenerExecutor = Executors.newSingleThreadExecutor();
    private static final ExecutorService serverExecutor = Executors.newWorkStealingPool();
    private static final Logger log = Logger.getLogger(UcxServer.class);
    private static final Stack<Closeable> resources = new Stack<>();
    private static UcpContext context;
    private static UcpWorker worker;
    private static InetSocketAddress serverAddress;
    private static UcxMemoryPool ucxMemoryPool;
    private static UcpListener listener;

    private UcxServer() {}

    public static void start(ShuffleServiceConfig shuffleServiceConfig)
    {
        serverAddress = new InetSocketAddress(shuffleServiceConfig.getHost(), shuffleServiceConfig.getPort());
        context = new UcpContext(new UcpParams()
                .requestWakeupFeature()
                .requestRmaFeature()
                .requestTagFeature()
                .setMtWorkersShared(true));
        resources.push(context);

        ucxMemoryPool = new UcxMemoryPool(context, 4096);
        ucxMemoryPool.preAlocate(UcxConstant.BASE_BUFFER_NB, UcxConstant.UCX_MIN_BUFFER_SIZE);
        resources.push(ucxMemoryPool);

        worker = context.newWorker(new UcpWorkerParams());
        resources.push(worker);

        listener = worker.newListener(new UcpListenerParams().setSockAddr(serverAddress));
        resources.push(listener);

        listenerExecutor.submit(new ListenerThread());
    }

    public static void shutdown()
    {
        log.info("Server " + serverAddress + " shutdown ...");
        if (!ListenerThread.interrupted()) {
            ListenerThread.currentThread().interrupt();
        }
        try {
            while (!resources.empty()) {
                resources.pop().close();
            }
        }
        catch (IOException e) {
            log.error("Server: shut down failed." + e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }

    private static class ListenerThread
            extends Thread
            implements Runnable
    {
        private UcpRequest recvRequest()
        {
            ByteBuffer setupMessageBuffer = ByteBuffer.allocateDirect(MAX_MESSAGE_SIZE);
            return worker.recvTaggedNonBlocking(setupMessageBuffer, new UcxCallback()
            {
                @Override
                public void onSuccess(UcpRequest request)
                {
                    UcxShuffleService shuffleService = new UcxShuffleService(context, ucxMemoryPool, setupMessageBuffer, serverExecutor);
                    serverExecutor.submit(shuffleService);
                }

                @Override
                public void onError(int ucsStatus, String errorMsg)
                {
                    super.onError(ucsStatus, errorMsg);
                }
            });
        }

        @Override
        public void run()
        {
            log.info("Server " + serverAddress + " start listening ...");
            UcpRequest recv = recvRequest();
            while (!isInterrupted()) {
                if (recv.isCompleted()) {
                    // Process 1 recv request at a time.
                    recv = recvRequest();
                }
                if (worker.progress() == 0) {
                    worker.waitForEvents();
                }
            }
            worker.cancelRequest(recv);
            recv.close();
            shutdown();
        }
    }
}
