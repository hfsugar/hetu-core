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

public class UcxConstant
{
    public static final int BYTE_SIZE = 1;
    public static final int LONG_SIZE = 8;
    public static final int INT_SIZE = 4;
    public static final int UCX_MIN_BUFFER_SIZE = 4096;
    public static final int BASE_BUFFER_NB = 4096;
    public static final int UCX_MAX_PRE_ALLOCATE = 1024*1024*128;
    public static final float DEFAULT_PREFETCH_COEFF = 0.5f;

    private UcxConstant() {}
}
