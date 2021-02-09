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
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

import static nova.hetu.shuffle.ucx.UcxConstant.INT_SIZE;
import static nova.hetu.shuffle.ucx.UcxConstant.UCX_MIN_BUFFER_SIZE;

public abstract class UcxMessage
{
    public static final int MAX_MESSAGE_SIZE = UCX_MIN_BUFFER_SIZE;
    public static final Charset CHARSET = StandardCharsets.UTF_8;
    public static final int MESSAGE_HEAD_SIZE = 4;

    UcxMessageType type;

    public UcxMessage(ByteBuffer data)
    {
        data.clear();
        type = UcxMessageType.values()[data.getInt()];
    }

    public static UcxMessageType parseType(ByteBuffer data)
    {
        data.clear();
        UcxMessageType type = UcxMessageType.values()[data.getInt()];
        data.clear();
        return type;
    }

    public UcxMessageType getType()
    {
        return type;
    }

    public enum UcxMessageType
    {
        CLOSE,
        SETUP,
        TAKE,
        PAGE
    }

    public static class Builder
    {
        private final UcxMemoryPool ucxMemoryPool;
        private UcxMessageType type;

        protected Builder(UcxMemoryPool ucxMemoryPool, UcxMessageType type)
        {
            this.ucxMemoryPool = ucxMemoryPool;
            this.type = type;
        }

        public RegisteredMemory build(int messageLength)
        {
            RegisteredMemory memory = ucxMemoryPool.get(INT_SIZE + messageLength);
            memory.getBuffer().putInt(type.ordinal());
            return memory;
        }
    }
}
