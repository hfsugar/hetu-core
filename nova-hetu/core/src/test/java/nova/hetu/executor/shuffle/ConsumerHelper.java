package nova.hetu.executor.shuffle;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.spi.Page;
import nova.hetu.shuffle.PageConsumer;
import nova.hetu.shuffle.ProducerInfo;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import static org.testng.Assert.assertEquals;

public class ConsumerHelper
{

    private final PageConsumer consumer;
    private final String uuid;
    private final long[] result;

    public ConsumerHelper(String host, int port, String taskid, int bufferid, PagesSerde serde, long[] result)
    {
        this.consumer = PageConsumer.create(new ProducerInfo(host, port, taskid + "-" + bufferid), serde, ShuffleServiceTestUtil.MAX_PAGE_SIZE_IN_BYTES, ShuffleServiceTestUtil.RATE_LIMIT);
        this.result = result;
        this.uuid = UUID.randomUUID().toString();
        System.out.println(uuid + " the comsumer uuid ");
    }

    public ConsumerHelper(String host, int port, String taskid, int bufferid, PagesSerde serde, long[] result, PagesSerde.CommunicationMode defaultCommMode)
    {
        this.consumer = PageConsumer.create(new ProducerInfo(host, port, taskid + "-" + bufferid), serde, defaultCommMode, ShuffleServiceTestUtil.MAX_PAGE_SIZE_IN_BYTES, ShuffleServiceTestUtil.RATE_LIMIT, true);
        this.result = result;
        this.uuid = UUID.randomUUID().toString();
        System.out.println(uuid + " the comsumer uuid ");
    }

    public PageConsumer getConsumer()
    {
        return consumer;
    }

    public long[] getResult()
    {
        return result;
    }

    public String getUuid()
    {
        return uuid;
    }

    public Thread createConsumerThread(int delay)
    {
        return new Thread(() -> {
            try {
                Thread.currentThread().setName("consumer thread " + uuid);
                Random random = new Random();
                int count = 0;
                while (true) {
                    Page page = consumer.poll();
                    if (page == null && consumer.isEnded()) {
                        // avoid stupid edge case poll no page -> get page and end -> check ended ... exit too early
                        page = consumer.poll();
                        if (page == null) {
                            break;
                        }
                    }
                    else if (page == null) {
                        continue;
                    }
                    count++;
                    int value = (int) page.getBlock(0).getLong(0, 0);
                    assertEquals(0, result[value], "received duplicate page");
                    result[value] = value;
//                    System.out.println(uuid + " Count : " + count +" taking the output: " + page + " content:" + page.getBlock(0).getLong(0, 0));
                    if (delay > 0) {
                        Thread.sleep(random.nextInt(delay));
                    }
                }
                System.out.println(uuid + " taking the output ended, :" + count);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public boolean isEnded()
    {
        return consumer.isEnded();
    }

    public void close()
            throws IOException
    {
        consumer.close();
    }
}
