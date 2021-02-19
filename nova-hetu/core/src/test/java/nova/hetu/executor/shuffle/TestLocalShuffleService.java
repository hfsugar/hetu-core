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

import com.google.common.collect.ImmutableList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.hetu.core.transport.execution.buffer.PagesSerde.CommunicationMode.UCX;
import static nova.hetu.executor.shuffle.ShuffleServiceTestUtil.TEST_SHUFFLE_SERVICE_HOST;
import static nova.hetu.executor.shuffle.ShuffleServiceTestUtil.TEST_SHUFFLE_SERVICE_PORT;
import static nova.hetu.shuffle.stream.Stream.Type.BASIC;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestLocalShuffleService
{
    @Test
    public void TestSingleProducerSingleConsumerQueue()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        ProducerHelper producer = new ProducerHelper(taskid, serde, BASIC);
        producer.addConsumers(ImmutableList.of(bufferid), true);
        Thread producerThread = producer.createProducerThread(0, 10, 10);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);
        consumerThread.start();
        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestSingleConsumerQueueNoProduderClose()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        long[] result = new long[10];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);
        consumerThread.start();
        assertFalse(consumer.isEnded());
        consumer.close();
        consumerThread.join();
        assertEquals(true, consumer.isEnded());
    }

    @Test
    public void TestSingleProducerSingleConsumerWithNoChannel()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        ProducerHelper producer = new ProducerHelper(taskid + "-" + bufferid, serde, BASIC);
        Thread producerThread = producer.createProducerThread(0, 10, 10);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);
        consumerThread.start();
        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestAddConsumerAfterProducerClose()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        ProducerHelper producer = new ProducerHelper(taskid, serde, BASIC);
        Thread producerThread = producer.createProducerThread(0, 10, 10);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);
        consumerThread.start();
        producer.addConsumers(ImmutableList.of(bufferid), true);

        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer.isEnded());
    }

    @Test
    public void TestNoOutputProducer()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;
        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();
        ProducerHelper producer = new ProducerHelper(taskid, serde, BASIC);
        producer.addConsumers(ImmutableList.of(bufferid), true);
        producer.close();

        long[] result = new long[0];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(0);
        consumerThread.start();
        consumerThread.join();
        assertEquals(true, consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    public void TestSingleProducerMultipleConsumerQueue()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        ProducerHelper producer = new ProducerHelper(taskid, serde, BASIC);
        producer.addConsumers(ImmutableList.of(bufferid), true);
        Thread producerThread = producer.createProducerThread(0, 10, 10);
        producerThread.start();

        producerThread.join();

        producer.close();

        long[] result = new long[10];
        long[] result2 = new long[10];

        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        ConsumerHelper consumer2 = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result2, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);
        Thread consumerThread2 = consumer2.createConsumerThread(10);
        consumerThread.start();
        consumerThread2.start();

        consumerThread.join();
        consumerThread2.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result2[i]);
        }
        assertEquals(true, consumer.isEnded());
        assertEquals(true, consumer2.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestMultiProducerSingleConsumerWithDelayQueue()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;
        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        long[] result = new long[40];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(10);

        ProducerHelper producer1 = new ProducerHelper(taskid, serde, BASIC);
        ProducerHelper producer2 = new ProducerHelper(taskid, serde, BASIC);
        producer1.addConsumers(ImmutableList.of(bufferid), true);

        Thread producer1Thread = producer1.createProducerThread(0, 20, 10);
        Thread producer2Thread = producer2.createProducerThread(20, 40, 10);

        producer1Thread.start();
        producer2Thread.start();
        consumerThread.start();

        producer1Thread.join();
        producer2Thread.join(); //wait for all producers to finish before exiting the try which closes the Out;

        producer1.close();
        producer2.close();

        consumerThread.join();

        while (!consumer.isEnded()) {
            Thread.sleep(50);
        }
        if (consumer.isEnded()) {
            //ensure all results received
            for (int i = 0; i < 40; i++) {
                assertEquals(i, result[i]);
            }
        }
        assertEquals(true, consumer.isEnded());
        assertTrue(producer1.isClosed());
        assertTrue(producer2.isClosed());
    }

    @Test
    public void TestMultiProducerSingleConsumerNoDelay()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;
        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        long[] result = new long[40];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);

        assertFalse(consumer.isEnded(), "page consumer started with ended status");

        Thread consumerThread = consumer.createConsumerThread(10);

        ProducerHelper producer1 = new ProducerHelper(taskid + "-" + bufferid, serde, BASIC);
        ProducerHelper producer2 = new ProducerHelper(taskid + "-" + bufferid, serde, BASIC);
        producer1.addConsumers(ImmutableList.of(bufferid), true);

        Thread producer1Thread = producer1.createProducerThread(0, 20, 10);
        Thread producer2Thread = producer2.createProducerThread(20, 40, 10);

        producer1Thread.start();
        producer2Thread.start();
        consumerThread.start();

        producer1Thread.join();
        producer2Thread.join(); //wait for all producers to finish before exiting the try which closes the Out;

        producer1.close();
        producer2.close();

        consumerThread.join();

        while (!consumer.isEnded()) {
            Thread.sleep(50);
        }
        if (consumer.isEnded()) {
            //ensure all results received
            for (int i = 0; i < 40; i++) {
                assertEquals(i, result[i]);
            }
        }
        assertEquals(true, consumer.isEnded());
        assertTrue(producer1.isClosed());
        assertTrue(producer2.isClosed());
    }

    @Test
    public void TestMultiProducerMultiConsumerNoDelay()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 1;
        int bufferId2 = 2;
        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        long[] result = new long[400];
        ConsumerHelper consumer1 = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferId1, serde, result, UCX, false);
        ConsumerHelper consumer2 = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferId2, serde, result, UCX, false);

        assertFalse(consumer1.isEnded(), "page consumer started with ended status");
        assertFalse(consumer2.isEnded(), "page consumer started with ended status");

        ProducerHelper producer1 = new ProducerHelper(taskid, serde, BASIC);
        ProducerHelper producer2 = new ProducerHelper(taskid, serde, BASIC);
        producer1.addConsumers(ImmutableList.of(bufferId1, bufferId2), true);

        Thread consumerThread1 = consumer1.createConsumerThread(10);
        Thread consumerThread2 = consumer2.createConsumerThread(10);

        Thread producer1Thread = producer1.createProducerThread(0, 200, 10);
        Thread producer2Thread = producer2.createProducerThread(200, 400, 10);

        producer1Thread.start();
        producer2Thread.start();
        consumerThread1.start();
        consumerThread2.start();
        producer1Thread.join();
        producer2Thread.join(); //wait for all producers to finish before exiting the try which closes the Out;
        producer1.close();
        producer2.close();
        consumerThread1.join();
        consumerThread2.join();

        while (!consumer1.isEnded() || !consumer2.isEnded()) {
            Thread.sleep(500);
        }
        //ensure all results received
        for (int i = 0; i < result.length; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer1.isEnded());
        assertEquals(true, consumer2.isEnded());
        assertTrue(producer1.isClosed());
        assertTrue(producer2.isClosed());
    }

    public void TestMultiProducerSingleConsumerQueue()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();
        long[] result = new long[4000];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(0);

        ProducerHelper producer1 = new ProducerHelper(taskid, serde, BASIC);
        ProducerHelper producer2 = new ProducerHelper(taskid, serde, BASIC);
        ProducerHelper producer3 = new ProducerHelper(taskid, serde, BASIC);
        ProducerHelper producer4 = new ProducerHelper(taskid, serde, BASIC);
        producer1.addConsumers(ImmutableList.of(bufferid), true);

        Thread producer1Thread = producer1.createProducerThread(0, 1000, 0);
        Thread producer2Thread = producer2.createProducerThread(1000, 2000, 0);
        Thread producer3Thread = producer3.createProducerThread(2000, 3000, 0);
        Thread producer4Thread = producer4.createProducerThread(3000, 4000, 0);

        producer1Thread.start();
        producer2Thread.start();
        producer3Thread.start();
        producer4Thread.start();

        consumerThread.start();

        producer1Thread.join();
        producer2Thread.join();
        producer3Thread.join();
        producer4Thread.join();

        producer1.close();
        producer2.close();
        producer3.close();
        producer4.close();

        consumerThread.join();

        while (!consumer.isEnded()) {
            TimeUnit.MILLISECONDS.sleep(50);
        }

        //ensure all results received
        for (int i = 0; i < 4000; i++) {
            assertEquals(i, result[i]);
        }
        assertTrue(consumer.isEnded());
        assertTrue(producer1.isClosed());
        assertTrue(producer2.isClosed());
        assertTrue(producer3.isClosed());
        assertTrue(producer4.isClosed());
    }

    @Test
    void TestPageConsumerStatus()
            throws InterruptedException
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();
        long[] result = new long[4000];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(0);

        assertFalse(consumer.isEnded(), "Consumer should not start with ended status");
        TimeUnit.MILLISECONDS.sleep(500);
        assertFalse(consumer.isEnded(), "Consumer should not be ended without getting any input");

        //potential half close situation when producer never published anything, not even EOS?? is this a legal problem?
    }

    @Test
    void TestConcurrentPageExchanges()
            throws ExecutionException, InterruptedException
    {
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        List<Future> results = new ArrayList<>();
        for (int i = 0; i < 1; i++) {
            Future future = executorService.submit(() -> {
                String threadName = Thread.currentThread().getName();
                try {
                    Thread.currentThread().setName("TestMultiProducerSingleConsumerQueue");
                    TestMultiProducerSingleConsumerQueue();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    Thread.currentThread().setName(threadName);
                }
            });
            results.add(future);
        }
        for (Future result : results) {
            result.get();
        }
        executorService.shutdown();
    }

    @Test
    public void TestConsumerStatus()
            throws Exception
    {
        String taskid = ShuffleServiceTestUtil.getTaskId();
        int bufferid = 0;
        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();
        ProducerHelper producer = new ProducerHelper(taskid + "-" + bufferid, serde, BASIC);
        producer.addConsumers(ImmutableList.of(bufferid), true);

        Thread producerThread = producer.createProducerThread(0, 10, 100);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskid, bufferid, serde, result, UCX, false);
        Thread consumerThread = consumer.createConsumerThread(0);
        consumerThread.start();
        consumerThread.join();
        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertEquals(true, consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestAddingConsumersMultipleTimes()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 0;
        int bufferId2 = 1;
        int totalPages = 2000;

        PagesSerde serde = new ShuffleServiceTestUtil.MockLocalConstantPagesSerde();

        ProducerHelper producer = new ProducerHelper(taskId, serde, BASIC);
        Thread producerThread = producer.createProducerThread(0, totalPages, 10);

        long[] result = new long[totalPages];

        ConsumerHelper consumer = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId, bufferId1, serde, result, UCX, false);
        ConsumerHelper consumer2 = new ConsumerHelper(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId, bufferId2, serde, result, UCX, false);

        Thread consumerThread = consumer.createConsumerThread(10);
        Thread consumerThread2 = consumer2.createConsumerThread(10);

        producerThread.start();
        consumerThread.start();
        consumerThread2.start();
        producer.addConsumers(ImmutableList.of(), false);
        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), false);
        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), false);

        producerThread.join();
        producer.close();
        consumerThread.join();
        consumerThread2.join();

        assertFalse(producer.isClosed());

        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), true);

        //ensure all results received
        for (int i = 0; i < totalPages; i++) {
            assertEquals(i, result[i]);
        }

        assertTrue(consumer.isEnded());
        assertTrue(consumer2.isEnded());
        assertTrue(producer.isClosed());
    }
}
