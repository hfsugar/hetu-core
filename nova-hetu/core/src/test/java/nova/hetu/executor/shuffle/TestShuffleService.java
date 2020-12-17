package nova.hetu.executor.shuffle;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockEncodingSerde;
import io.prestosql.spi.block.LongArrayBlock;
import nova.hetu.GrpcServer;
import nove.hetu.executor.PageConsumer;
import nove.hetu.executor.PageProducer;
import nove.hetu.executor.ShuffleService;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestShuffleService
{
    private static Random random = new Random();

    @Test
    public void TestSingleProducerSingleConsumerQueue()
            throws InterruptedException
    {
        GrpcServer.start();
        String taskid = "taskid";
        String bufferid = "0";

        PagesSerde serde = new MockConstantPagesSerde();
        try (ShuffleService.Stream out = ShuffleService.getStream(taskid, bufferid, serde)) {
            PageProducer producer = new PageProducer(out);
            Thread producerThread = createProducerThread(producer, 0, 10, 100);
            producerThread.start();
            producerThread.join();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        long[] result = new long[10];
        PageConsumer consumer = new PageConsumer(taskid, bufferid, serde);
        Thread consumerThread = createConsumerThread(consumer, result, 100);
        consumerThread.start();
        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer.ended());
    }

    @Test
    public void TestMultiProducerSingleConsumerWithDelayQueue()
            throws InterruptedException
    {
        GrpcServer.start();
        String taskid = "taskid";
        String bufferid = "0";
        PagesSerde serde = new MockConstantPagesSerde();

        long[] result = new long[40];
        PageConsumer consumer = new PageConsumer(taskid, bufferid, serde);
        Thread consumerThread = createConsumerThread(consumer, result, 100);

        try (ShuffleService.Stream out = ShuffleService.getStream(taskid, bufferid, serde)) {

            PageProducer producer1 = new PageProducer(out);
            PageProducer producer2 = new PageProducer(out);

            Thread producer1Thread = createProducerThread(producer1, 0, 20, 100);
            Thread producer2Thread = createProducerThread(producer2, 20, 40, 100);

            producer1Thread.start();
            producer2Thread.start();
            consumerThread.start();

            producer1Thread.join();
            producer2Thread.join(); //wait for all producers to finish before exiting the try which closes the Out;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        consumerThread.join();

        while (!consumer.ended()) {
            Thread.sleep(50);
        }
        if (consumer.ended()) {
            //ensure all results received
            for (int i = 0; i < 40; i++) {
                assertEquals(i, result[i]);
            }
        }
        assertEquals(true, consumer.ended());
    }

    @Test
    public void TestMultiProducerSingleConsumerNoDelay()
            throws InterruptedException
    {
        GrpcServer.start();
        String taskid = "taskid";
        String bufferid = "0";
        PagesSerde serde = new MockConstantPagesSerde();

        long[] result = new long[40];
        PageConsumer consumer = new PageConsumer(taskid, bufferid, serde);

        assertFalse("page consumer started with ended status", consumer.ended());

        Thread consumerThread = createConsumerThread(consumer, result, 100);

        try (ShuffleService.Stream out = ShuffleService.getStream(taskid, bufferid, serde)) {

            PageProducer producer1 = new PageProducer(out);
            PageProducer producer2 = new PageProducer(out);

            Thread producer1Thread = createProducerThread(producer1, 0, 20, 100);
            Thread producer2Thread = createProducerThread(producer2, 20, 40, 100);

            producer1Thread.start();
            producer2Thread.start();
            consumerThread.start();

            producer1Thread.join();
            producer2Thread.join(); //wait for all producers to finish before exiting the try which closes the Out;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        consumerThread.join();

        while (!consumer.ended()) {
            Thread.sleep(50);
        }
        if (consumer.ended()) {
            //ensure all results received
            for (int i = 0; i < 40; i++) {
                assertEquals(i, result[i]);
            }
        }
        assertEquals(true, consumer.ended());
    }

    @Test
    public void TestMultiProducerSingleConsumerQueue()
            throws InterruptedException
    {
        GrpcServer.start();
        String taskid = "taskid";
        String bufferid = "0";

        PagesSerde serde = new MockConstantPagesSerde();
        long[] result = new long[4000];
        PageConsumer consumer = new PageConsumer(taskid, bufferid, serde);
        Thread consumerThread = createConsumerThread(consumer, result, 0);

        try (ShuffleService.Stream stream = ShuffleService.getStream(taskid, bufferid, serde)) {

            PageProducer producer1 = new PageProducer(stream);
            PageProducer producer2 = new PageProducer(stream);
            PageProducer producer3 = new PageProducer(stream);
            PageProducer producer4 = new PageProducer(stream);

            Thread producer1Thread = createProducerThread(producer1, 0, 1000, 0);
            Thread producer2Thread = createProducerThread(producer2, 1000, 2000, 0);
            Thread producer3Thread = createProducerThread(producer3, 2000, 3000, 0);
            Thread producer4Thread = createProducerThread(producer4, 3000, 4000, 0);

            producer1Thread.start();
            producer2Thread.start();
            producer3Thread.start();
            producer4Thread.start();

            consumerThread.start();

            producer1Thread.join();
            producer2Thread.join();
            producer3Thread.join();
            producer4Thread.join();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }

        consumerThread.join();

        while (!consumer.ended()) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        //ensure all results received
        for (int i = 0; i < 4000; i++) {
            assertEquals(i, result[i]);
        }
        assertTrue(consumer.ended());
    }

    @Test
    void TestPageConsumerStatus()
            throws InterruptedException
    {
        GrpcServer.start();
        String taskid = "taskid";
        String bufferid = "0";

        PagesSerde serde = new MockConstantPagesSerde();
        long[] result = new long[4000];
        PageConsumer consumer = new PageConsumer(taskid, bufferid, serde);
        Thread consumerThread = createConsumerThread(consumer, result, 0);

        assertFalse("Consumer should not start with ended status", consumer.ended());
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse("Consumer should not be ended without getting any input", consumer.ended());
    }

    static Thread createConsumerThread(PageConsumer consumer, long[] result, int delay)
    {
        return new Thread(new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    int count=0;
                    while (!consumer.ended()) {
                        count++;
                        Page page = consumer.take();
                        int value = (int) page.getBlock(0).getLong(0, 0);
                        assertEquals("received duplicate page", 0, result[value]);
                        result[value] = value;
                        System.out.println("taking the output: " + page + " content:" + page.getBlock(0).getLong(0, 0));
                        if (delay > 0) {
                            Thread.sleep(random.nextInt(delay));
                        }
                    }
                    System.out.println("taking the output ended, :" + count);

                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
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
                    for (int i = start; i < end; i++) {
                        producer.send(getPage(i));
                        if (delay > 0) {
                            Thread.sleep(random.nextInt(delay));
                        }
                    }
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    static Page getPage(int count)
    {
        long[] values = new long[] {count};
        Block block = new LongArrayBlock(1, Optional.empty(), values);
        return new Page(block);
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
}
