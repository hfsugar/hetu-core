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
package nova.hetu.executor.shuffle;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlock;

import java.util.Optional;
import java.util.Random;

import static com.google.common.base.Preconditions.checkArgument;

public class ShuffleServiceTestUtil
{
    public static final int TEST_SHUFFLE_SERVICE_PORT = 16544;
    public static final String TEST_SHUFFLE_SERVICE_HOST = "127.0.1.1";
    public static final int MAX_PAGE_SIZE_IN_BYTES = 1024*1024;
    public static final int RATE_LIMIT = 64;

    private ShuffleServiceTestUtil() {}

    static Page getPage(int count)
    {
        long[] values = new long[] {count};
        Block block = new LongArrayBlock(1, Optional.empty(), values);
        return new Page(block);
    }

    static String getTaskId()
    {
        Random random = new Random();
        return "task-" + random.nextInt(10000);
    }

    static class MockConstantPagesSerde
            extends PagesSerde
    {
        MockConstantPagesSerde()
        {
            super(new MockBlockEncodingSerde(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        @Override
        public SerializedPage serialize(Page page)
        {
            String strValue = String.valueOf(page.getBlock(0).getLong(0, 0));
            return new SerializedPage(strValue.getBytes(), (byte) 0, page.getPositionCount(), strValue.getBytes().length);
        }

        @Override
        public Page deserialize(SerializedPage serializedPage)
        {
            serializedPage.getPositionCount();
            long[] values = new long[] {Long.valueOf(new String(serializedPage.getSliceArray()))};
            LongArrayBlock block = new LongArrayBlock(1, Optional.empty(), values);
            return new Page(block);
        }
    }

    static class MockBlockEncodingSerde
            implements BlockEncodingSerde
    {
        @Override
        public Block readBlock(SliceInput input)
        {
            return null;
        }

        @Override
        public void writeBlock(SliceOutput output, Block block)
        {
        }
    }

    static class MockLocalConstantPagesSerde
            extends PagesSerde
    {
        MockLocalConstantPagesSerde()
        {
            super(new MockBlockEncodingSerde(), Optional.empty(), Optional.empty(), Optional.empty());
        }

        @Override
        public SerializedPage serialize(Page page)
        {
            String strValue = String.valueOf(page.getBlock(0).getLong(0, 0));
            return new SerializedPage(strValue.getBytes(), (byte) 0, page.getPositionCount(), strValue.getBytes().length, page);
        }

        @Override
        public Page deserialize(SerializedPage serializedPage)
        {
            Page page = serializedPage.getRawPageReference();
            checkArgument(page != null, "Page is null");
            return page;
        }
    }
}
