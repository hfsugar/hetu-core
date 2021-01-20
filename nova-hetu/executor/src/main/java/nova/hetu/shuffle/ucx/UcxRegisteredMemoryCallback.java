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

import nova.hetu.shuffle.ucx.memory.RegisteredMemory;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;
import org.openucx.jucx.UcxCallback;
import org.openucx.jucx.ucp.UcpRequest;

public class UcxRegisteredMemoryCallback
        extends UcxCallback
{
    private final RegisteredMemory memory;
    private final UcxMemoryPool ucxMemoryPool;

    public UcxRegisteredMemoryCallback(RegisteredMemory memory, UcxMemoryPool ucxMemoryPool)
    {
        this.memory = memory;
        this.ucxMemoryPool = ucxMemoryPool;
    }

    @Override
    public void onSuccess(UcpRequest request)
    {
        super.onSuccess(request);
        this.ucxMemoryPool.put(memory);
    }

    @Override
    public void onError(int ucsStatus, String errorMsg)
    {
        this.ucxMemoryPool.put(memory);
        super.onError(ucsStatus, errorMsg);
    }
}
