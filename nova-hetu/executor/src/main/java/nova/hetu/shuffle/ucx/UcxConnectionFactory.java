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

import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpParams;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UcxConnectionFactory
        implements Closeable
{
    private static final ExecutorService executor = Executors.newWorkStealingPool();
    private final ConcurrentMap<String, UcxConnection> allConnections = new ConcurrentHashMap<>();
    private final UcpContext context;
    private final UcxMemoryPool ucxMemoryPool;

    public UcxConnectionFactory()
    {
        this.context = new UcpContext(new UcpParams().requestWakeupFeature()
                .requestRmaFeature()
                .requestTagFeature()
                .setMtWorkersShared(true));
        this.ucxMemoryPool = new UcxMemoryPool(context, UcxConstant.BASE_BUFFER_NB * UcxConstant.UCX_MIN_BUFFER_SIZE, UcxConstant.UCX_MIN_BUFFER_SIZE, "CONNECTION-GENERAL");
        this.ucxMemoryPool.preAllocate(UcxConstant.BASE_BUFFER_NB, UcxConstant.UCX_MAX_MSG_SIZE);
    }

    public UcxMemoryPool getMemoryPool()
    {
        return ucxMemoryPool;
    }

    public synchronized UcxConnection getOrCreateConnection(String host, int port)
    {
        InetSocketAddress address = new InetSocketAddress(host, port);
        UcxConnection connection = allConnections.get(address.getHostName());

        if (connection != null && !connection.isClosed()) {
            return connection;
        }
        UcxConnection newConnection = new UcxConnection(context, address, this);
        if (connection == null) {
            allConnections.put(address.getHostName(), newConnection);
        }
        else {
            allConnections.replace(address.getHostName(), newConnection);
        }
        return newConnection;
    }

    public ExecutorService getExecutor()
    {
        return executor;
    }

    @Override
    public void close()
            throws IOException
    {
        for (UcxConnection connection : allConnections.values()) {
            connection.close();
        }
        this.ucxMemoryPool.close();
        this.context.close();
    }

    public synchronized void closeConnection(UcxConnection ucxConnection)
    {
        if (ucxConnection == this.allConnections.get(ucxConnection.getHost())) {
            this.allConnections.remove(ucxConnection.getHost());
        }
    }
}
