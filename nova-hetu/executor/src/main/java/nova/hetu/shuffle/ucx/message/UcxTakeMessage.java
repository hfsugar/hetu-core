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

package nova.hetu.shuffle.ucx.message;

import nova.hetu.shuffle.ucx.memory.RegisteredMemory;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;

import java.nio.ByteBuffer;

import static nova.hetu.shuffle.ucx.UcxConstant.INT_SIZE;
import static nova.hetu.shuffle.ucx.message.UcxMessage.UcxMessageType.TAKE;

public class UcxTakeMessage
        extends UcxMessage
{
    // | id(4B) | rateLimit(4B) | producerIdSize(4B) | producerId(producerIdSize Bytes) |
    private final int id;
    private final int rateLimit;
    private final int numProcessed;
    private final String producerId;

    @Override
    public int getMaxMessageSize()
    {
        return INT_SIZE * 4 + MAX_PRODUCER_ID_SIZE;
    }

    public UcxTakeMessage(ByteBuffer data)
    {
        super(data);
        this.id = data.getInt();
        this.rateLimit = data.getInt();
        this.numProcessed = data.getInt();
        int producerIdSize = data.getInt();
        data.limit(data.position() + producerIdSize);
        this.producerId = CHARSET.decode(data).toString();
    }

    public int getId()
    {
        return id;
    }

    public int getRateLimit()
    {
        return rateLimit;
    }

    public int getNumProcessed()
    {
        return numProcessed;
    }

    public String getProducerId()
    {
        return producerId;
    }

    @Override
    public String toString()
    {
        return "TAKE message:{" +
                "id:" +
                id +
                " producerId:" +
                producerId +
                " rateLimit:" +
                rateLimit +
                "}";
    }

    public static class Builder
            extends UcxMessage.Builder
    {
        private int rateLimit;
        private int id;
        private int numProcessed;
        private String producerId;

        public Builder(UcxMemoryPool ucxMemoryPool)
        {
            super(ucxMemoryPool, TAKE);
        }

        public Builder setRateLimit(int rateLimit)
        {
            this.rateLimit = rateLimit;
            return this;
        }

        public Builder setId(int id)
        {
            this.id = id;
            return this;
        }

        public Builder setNumProcessed(int numProcessed)
        {
            this.numProcessed = numProcessed;
            return this;
        }

        public Builder setProducerId(String producerId)
        {
            this.producerId = producerId;
            return this;
        }

        public RegisteredMemory build()
        {
            int producerIdSize = producerId.getBytes(CHARSET).length;
            int bufferSize = INT_SIZE * 4 + producerIdSize;
            RegisteredMemory memory = build(bufferSize);
            ByteBuffer buffer = memory.getBuffer();
            buffer.putInt(id);
            buffer.putInt(rateLimit);
            buffer.putInt(numProcessed);
            buffer.putInt(producerIdSize);
            buffer.put(producerId.getBytes(CHARSET));
            buffer.clear();
            return memory;
        }
    }
}
