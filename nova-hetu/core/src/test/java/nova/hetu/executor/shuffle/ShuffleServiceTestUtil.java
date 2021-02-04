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
import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlock;
import nova.hetu.shuffle.PageConsumer;
import nova.hetu.shuffle.PageProducer;

import java.util.Optional;
import java.util.Random;

import static org.testng.Assert.assertEquals;

public class ShuffleServiceTestUtil
{
    public static final int TEST_SHUFFLE_SERVICE_PORT = 16544;
    public static final String TEST_SHUFFLE_SERVICE_HOST = "127.0.1.1";

    private ShuffleServiceTestUtil() {}

    static Page getPage(int count)
    {
        long[] values = new long[] {count};
        Block block = new LongArrayBlock(1, Optional.empty(), values);
        return new Page(block);
    }

    static Thread createConsumerThread(PageConsumer consumer, long[] result, int delay)
    {
        return new Thread(() -> {
            try {
                Thread.currentThread().setName("consumer thread");
                Random random = new Random();
                int count = 0;
                while (!consumer.isEnded()) {
                    Page page = consumer.poll();
                    if (page == null) {
                        continue;
                    }
                    count++;
                    int value = (int) page.getBlock(0).getLong(0, 0);
                    assertEquals(0, result[value], "received duplicate page");
                    result[value] = value;
//                    System.out.println(consumer.toString() + " taking the output: " + page + " content:" + page.getBlock(0).getLong(0, 0));
                    if (delay > 0) {
                        Thread.sleep(random.nextInt(delay));
                    }
                }
                System.out.println("taking the output ended, :" + count);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * The test assumes the page value will be int and the values[idx] = idx;
     * This is to simplify the verification of the transport layer correctness
     *
     * @param producer
     * @param start start value
     * @param end end value
     * @param delay amount of delay between sending pages
     * @return
     */
    static Thread createProducerThread(PageProducer producer, int start, int end, int delay)
    {
        return new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    Random random = new Random();
                    System.out.println("Producing pages from " + start + " to " + end + " with delay " + delay);
                    for (int i = start; i < end; i++) {
//                        System.out.println("sedning: " + i);
                        producer.send(getPage(i));
                        if (delay > 0) {
                            Thread.sleep(random.nextInt(delay));
                        }
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                catch (Exception e) {
                    System.out.println(e.getMessage());
                }
            }
        });
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
            return new SerializedPage(page.getBlocks(), PageCodecMarker.MarkerSet.empty(), page.getPositionCount(), (int) page.getSizeInBytes(), null);
        }

        @Override
        public Page deserialize(SerializedPage serializedPage)
        {
            return new Page(serializedPage.getBlocks());
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
}
