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

public class PageSerializeUtil
{
    private PageSerializeUtil() {}

    public static SerializedPage serialize(PagesSerde serde, Page page)
    {
        if (page.isOffHeap()) {
            return new SerializedPage(page.getBlocks(), PageCodecMarker.MarkerSet.empty(), page.getPositionCount(), (int) page.getSizeInBytes(), page.getPageMetadata());
        }
        SerializedPage serializedPage = serde.serialize(page);
        page.free();
        return serializedPage;
    }

    public static Page deserialize(PagesSerde serde, SerializedPage serializedPage)
    {
        if (serializedPage.isOffHeap()) {
            return new Page(serializedPage.getPositionCount(), serializedPage.getPageMetadata(), serializedPage.getBlocks());
        }
        return serde.deserialize(serializedPage);
    }
}
