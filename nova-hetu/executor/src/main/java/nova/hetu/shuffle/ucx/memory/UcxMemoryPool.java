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

import nova.hetu.shuffle.ucx.UcxConstant;
import org.apache.log4j.Logger;
import org.openucx.jucx.UcxException;
import org.openucx.jucx.UcxUtils;
import org.openucx.jucx.ucp.UcpContext;
import org.openucx.jucx.ucp.UcpMemMapParams;
import org.openucx.jucx.ucp.UcpMemory;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;

public class UcxMemoryPool
        implements Closeable
{
    private static final int defaultBufferSize = UcxConstant.UCX_MIN_BUFFER_SIZE;
    private static final Logger logger = Logger.getLogger(UcxMemoryPool.class);
    private static final Constructor<?> directBufferConstructor;
    private final ConcurrentHashMap<Integer, AllocatorStack> allocStackMap =
            new ConcurrentHashMap<>();
    private final UcpContext context;
    private final int minBufferSize;

    public UcxMemoryPool(UcpContext context, int minBufferSize)
    {
        this.context = context;
        this.minBufferSize = minBufferSize;
    }

    private static ByteBuffer getByteBuffer(long address, int length)
            throws IOException
    {
        try {
            return (ByteBuffer) directBufferConstructor.newInstance(address, length);
        }
        catch (InvocationTargetException ex) {
            throw new IOException("java.nio.DirectByteBuffer: " +
                    "InvocationTargetException: " + ex.getTargetException());
        }
        catch (Exception e) {
            throw new IOException("java.nio.DirectByteBuffer exception: " + e.getMessage());
        }
    }

    @Override
    public void close()
    {
        for (AllocatorStack stack : allocStackMap.values()) {
            stack.close();
        }
        allocStackMap.clear();
    }

    private long roundUpToTheNextPowerOf2(long length)
    {
        // Round up length to the nearest power of two, or the minimum block size
        if (length < minBufferSize) {
            length = minBufferSize;
        }
        else {
            length--;
            length |= length >> 1;
            length |= length >> 2;
            length |= length >> 4;
            length |= length >> 8;
            length |= length >> 16;
            length++;
        }
        return length;
    }

    public RegisteredMemory get(int size)
    {
        long roundedSize = roundUpToTheNextPowerOf2(size);
        AllocatorStack stack =
                allocStackMap.computeIfAbsent((int) roundedSize, AllocatorStack::new);
        RegisteredMemory result = stack.get();
        result.getBuffer().position(0).limit(size);
        return result;
    }

    public void put(RegisteredMemory memory)
    {
        AllocatorStack allocatorStack = allocStackMap.get(memory.getBuffer().capacity());
        if (allocatorStack != null) {
            allocatorStack.put(memory);
        }
    }

    public void preAlocate(int numBuffers, int size)
    {
        AllocatorStack stack = new AllocatorStack(size);
        allocStackMap.put(size, stack);
        stack.preallocate(numBuffers);
    }

    private class AllocatorStack
            implements Closeable
    {
        private final AtomicInteger totalRequests = new AtomicInteger(0);
        private final AtomicInteger totalAlloc = new AtomicInteger(0);
        private final AtomicInteger preAllocs = new AtomicInteger(0);
        private final ConcurrentLinkedDeque<RegisteredMemory> stack = new ConcurrentLinkedDeque<>();
        private final int length;
        private final int minRegistrationSize;

        private AllocatorStack(int length)
        {
            this.length = length;
            this.minRegistrationSize = defaultBufferSize;
        }

        private RegisteredMemory get()
        {
            RegisteredMemory result = stack.pollFirst();
            if (result == null) {
                if (length < minRegistrationSize) {
                    int numBuffers = minRegistrationSize / length;
                    preallocate(numBuffers);
                    result = stack.pollFirst();
                    if (result == null) {
                        return get();
                    }
                    else {
                        result.getRefCount().incrementAndGet();
                    }
                }
                else {
                    UcpMemMapParams memMapParams = new UcpMemMapParams().setLength(length).allocate();
                    UcpMemory memory = context.memoryMap(memMapParams);
                    ByteBuffer buffer;
                    try {
                        buffer = UcxUtils.getByteBufferView(memory.getAddress(), (int) memory.getLength());
                    }
                    catch (Exception e) {
                        throw new UcxException(e.getMessage());
                    }
                    result = new RegisteredMemory(new AtomicInteger(1), memory, buffer);
                    totalAlloc.incrementAndGet();
                }
            }
            else {
                result.getRefCount().incrementAndGet();
            }
            totalRequests.incrementAndGet();
            return result;
        }

        private void put(RegisteredMemory registeredMemory)
        {
            registeredMemory.getRefCount().decrementAndGet();
            stack.addLast(registeredMemory);
        }

        private void preallocate(int numBuffers)
        {
            if ((long) length * (long) numBuffers > Integer.MAX_VALUE) {
                numBuffers = Integer.MAX_VALUE / length;
            }

            UcpMemMapParams memMapParams = new UcpMemMapParams().allocate().setLength(numBuffers * (long) length);
            UcpMemory memory = context.memoryMap(memMapParams);
            ByteBuffer buffer;
            try {
                buffer = getByteBuffer(memory.getAddress(), numBuffers * length);
            }
            catch (Exception ex) {
                throw new UcxException(ex.getMessage());
            }

            AtomicInteger refCount = new AtomicInteger(numBuffers);
            for (int i = 0; i < numBuffers; i++) {
                buffer.position(i * length).limit(i * length + length);
                final ByteBuffer slice = buffer.slice();
                RegisteredMemory registeredMemory = new RegisteredMemory(refCount, memory, slice);
                put(registeredMemory);
            }
            preAllocs.incrementAndGet();
            totalAlloc.incrementAndGet();
        }

        @Override
        public void close()
        {
            while (!stack.isEmpty()) {
                RegisteredMemory memory = stack.pollFirst();
                if (memory != null) {
                    memory.deregisterNativeMemory();
                }
            }
        }
    }

    static {
        try {
            Class<?> classDirectByteBuffer = Class.forName("java.nio.DirectByteBuffer");
            directBufferConstructor = classDirectByteBuffer.getDeclaredConstructor(long.class, int.class);
            directBufferConstructor.setAccessible(true);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
