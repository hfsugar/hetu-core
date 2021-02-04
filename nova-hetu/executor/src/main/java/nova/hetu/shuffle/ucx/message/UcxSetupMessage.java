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
import static nova.hetu.shuffle.ucx.message.UcxMessage.UcxMessageType.SETUP;

public class UcxSetupMessage
        extends UcxMessage
{
    // | ucpWorkerAddressSize(4B) | ucpWorkerAddress |
    private final ByteBuffer ucpWorkerAddress;

    public UcxSetupMessage(ByteBuffer data)
    {
        super(data);
        int ucpWorkerAddressSize = data.getInt();
        data.limit(data.position() + ucpWorkerAddressSize);
        this.ucpWorkerAddress = data.slice();
    }

    public ByteBuffer getUcpWorkerAddress()
    {
        return ucpWorkerAddress;
    }

    public static class Builder
            extends UcxMessage.Builder
    {
        private ByteBuffer ucpWorkerAddress;

        public Builder(UcxMemoryPool ucxMemoryPool)
        {
            super(ucxMemoryPool, SETUP);
        }

        public Builder setUcpWorkerAddress(ByteBuffer ucpWorkerAddress)
        {
            this.ucpWorkerAddress = ucpWorkerAddress;
            return this;
        }

        public RegisteredMemory build()
        {
            // | ucpWorkerAddressSize(4B) | ucpWorkerAddress |
            RegisteredMemory memory = build(INT_SIZE + ucpWorkerAddress.capacity());
            ByteBuffer buffer = memory.getBuffer();
            buffer.putInt(ucpWorkerAddress.capacity());
            buffer.put(ucpWorkerAddress);
            buffer.clear();
            return memory;
        }
    }
}
