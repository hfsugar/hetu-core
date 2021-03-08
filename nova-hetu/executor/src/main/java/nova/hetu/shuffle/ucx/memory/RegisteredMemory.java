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
package nova.hetu.shuffle.ucx.memory;

import org.apache.log4j.Logger;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpMemory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class RegisteredMemory
        implements Closeable
{
    private static final Logger logger = Logger.getLogger(RegisteredMemory.class);

    private final AtomicInteger refcount;
    private final UcpMemory memory;
    private final ByteBuffer buffer;
    private final UcxMemoryPool ucxMemoryPool;

    RegisteredMemory(UcxMemoryPool ucxMemoryPool, AtomicInteger refcount, UcpMemory memory, ByteBuffer buffer)
    {
        this.refcount = refcount;
        this.memory = memory;
        this.buffer = buffer;
        this.ucxMemoryPool = ucxMemoryPool;
    }

    public ByteBuffer getBuffer()
    {
        return this.buffer;
    }

    AtomicInteger getRefCount()
    {
        return refcount;
    }

    void deregisterNativeMemory()
    {
        if (refcount.get() != 0) {
            logger.warn("De-registering memory of size " + buffer.capacity() + " that has active references on Memory pool" + ucxMemoryPool.getName());
        }
        if (memory != null && memory.getNativeId() != null) {
            memory.deregister();
        }
    }

    public ByteBuffer getRemoteKeyBuffer()
    {
        return memory.getRemoteKeyBuffer();
    }

    public long getAddress()
    {
        return UcxUtils.getAddress(buffer);
    }

    @Override
    public void close()
            throws IOException
    {
        this.buffer.clear();
        ucxMemoryPool.put(this);
    }
}
