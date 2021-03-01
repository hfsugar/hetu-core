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

package nova.hetu.shuffle.stream;

import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import static nova.hetu.shuffle.ucx.UcxConstant.MIN_OFF_HEAP_TRANSFER;
import static nova.hetu.shuffle.ucx.UcxConstant.UCX_MAX_MSG_SIZE;
import static nova.hetu.shuffle.ucx.message.UcxMessage.MAX_RKEY_SIZE;

public class PageSerializeUtil
{
    private PageSerializeUtil() {}

    public static SerializedPage serialize(PagesSerde serde, Page page, PagesSerde.CommunicationMode commMode)
    {
        // page is off heap and off heap enabled
        if (page.isOffHeap() && commMode == PagesSerde.CommunicationMode.UCX) {
            // we are of a decent size
            if (page.getSizeInBytes() >= MIN_OFF_HEAP_TRANSFER) {
                // we do not have too many chunks
                if (page.getBlocks().length * MAX_RKEY_SIZE < UCX_MAX_MSG_SIZE) {
                    return new SerializedPage(page.getBlocks(), PageCodecMarker.MarkerSet.empty(), page.getPositionCount(), (int) page.getSizeInBytes(), page.getPageMetadata(), page);
                }
            }
        }
        return serde.serialize(page);
    }

    public static Page deserialize(PagesSerde serde, SerializedPage serializedPage)
    {
        return serde.deserialize(serializedPage);
    }
}
