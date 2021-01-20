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
import nova.hetu.RsServer;
import nova.hetu.ShuffleServiceConfig;
import nova.hetu.shuffle.PageConsumer;
import nova.hetu.shuffle.PageProducer;
import nova.hetu.shuffle.ProducerInfo;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

import static nova.hetu.executor.shuffle.ShuffleServiceTestUtil.TEST_SHUFFLE_SERVICE_HOST;
import static nova.hetu.executor.shuffle.ShuffleServiceTestUtil.TEST_SHUFFLE_SERVICE_PORT;
import static nova.hetu.shuffle.stream.Stream.Type.BROADCAST;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestBroadcastStream
{
    @BeforeSuite
    public void setup()
            throws InterruptedException
    {
        RsServer.start(new ShuffleServiceConfig());
    }

    @AfterSuite
    public void tearDown() {}

    @Test
    public void TestSingleProducerSingleConsumer()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        producer.addConsumers(ImmutableList.of(bufferId), true);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10, 10);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 10);
        consumerThread.start();
        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        assertTrue(consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestSingleProducerMultipleConsumer()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 0;
        int bufferId2 = 1;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), true);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10, 10);
        producerThread.start();
        producerThread.join();

        producer.close();

        long[] result = new long[10];
        long[] result2 = new long[10];

        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId1), serde);
        PageConsumer consumer2 = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId2), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 10);
        Thread consumerThread2 = ShuffleServiceTestUtil.createConsumerThread(consumer2, result2, 10);
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
        assertTrue(consumer.isEnded());
        assertTrue(consumer2.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestSingleProducerMultipleConsumerJoinedWithDelay()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 0;
        int bufferId2 = 1;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), true);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10, 10);

        long[] result = new long[10];
        long[] result2 = new long[10];

        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId1), serde);
        PageConsumer consumer2 = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId2), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 10);
        Thread consumerThread2 = ShuffleServiceTestUtil.createConsumerThread(consumer2, result2, 10);
        consumerThread.start();
        producerThread.start();
        producerThread.join();

        producer.close();
        Thread.sleep(500);
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
        assertTrue(consumer.isEnded());
        assertTrue(consumer2.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestSingleProducerMultipleConsumerAllAtOnce()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 0;
        int bufferId2 = 1;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        producer.addConsumers(ImmutableList.of(bufferId1, bufferId2), true);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10, 10);

        long[] result = new long[10];
        long[] result2 = new long[10];

        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId1), serde);
        PageConsumer consumer2 = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId2), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 10);
        Thread consumerThread2 = ShuffleServiceTestUtil.createConsumerThread(consumer2, result2, 10);

        producerThread.start();
        consumerThread.start();
        consumerThread2.start();

        producerThread.join();
        producer.close();
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
        assertTrue(consumer.isEnded());
        assertTrue(consumer2.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestBigStream()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId = 0;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        producer.addConsumers(ImmutableList.of(bufferId), true);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10000, 0);
        producerThread.start();

        long[] result = new long[10000];
        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 0);
        consumerThread.start();
        producerThread.join();

        producer.close();
        consumerThread.join();

        //ensure all results received
        for (int i = 0; i < 10000; i++) {
            assertEquals(i, result[i]);
        }
        assertTrue(consumer.isEnded());
        assertTrue(producer.isClosed());
    }

    @Test
    public void TestAddingConsumersMultipleTimes()
            throws Exception
    {
        String taskId = ShuffleServiceTestUtil.getTaskId();
        int bufferId1 = 0;
        int bufferId2 = 1;

        PagesSerde serde = new ShuffleServiceTestUtil.MockConstantPagesSerde();

        PageProducer producer = new PageProducer(taskId, serde, BROADCAST);
        Thread producerThread = ShuffleServiceTestUtil.createProducerThread(producer, 0, 10, 10);

        long[] result = new long[10];
        long[] result2 = new long[10];

        PageConsumer consumer = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId1), serde);
        PageConsumer consumer2 = PageConsumer.create(new ProducerInfo(TEST_SHUFFLE_SERVICE_HOST, TEST_SHUFFLE_SERVICE_PORT, taskId + "-" + bufferId2), serde);
        Thread consumerThread = ShuffleServiceTestUtil.createConsumerThread(consumer, result, 10);
        Thread consumerThread2 = ShuffleServiceTestUtil.createConsumerThread(consumer2, result2, 10);

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
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result[i]);
        }
        //ensure all results received
        for (int i = 0; i < 10; i++) {
            assertEquals(i, result2[i]);
        }
        assertTrue(consumer.isEnded());
        assertTrue(consumer2.isEnded());
        assertTrue(producer.isClosed());
    }
}
