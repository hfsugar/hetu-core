package nova.hetu.executor.shuffle;

import com.google.common.collect.ImmutableList;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import nova.hetu.shuffle.PageProducer;
import nova.hetu.shuffle.stream.Stream;

import java.util.Random;

import static nova.hetu.executor.shuffle.ShuffleServiceTestUtil.getPage;

public class ProducerHelper
{

    private final PageProducer producer;

    public ProducerHelper(String taskid, PagesSerde serde, Stream.Type type)
    {
        producer = new PageProducer(taskid, serde, type);
    }

    /**
     * The test assumes the page value will be int and the values[idx] = idx;
     * This is to simplify the verification of the transport layer correctness
     *
     * @return
     */
    public Thread createProducerThread(int start, int end, int delay)
    {
        return new Thread(() -> {
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
        });
    }

    public void close()
            throws Exception
    {
        producer.close();
    }

    public void addConsumers(ImmutableList<Integer> of, boolean b)
            throws InterruptedException
    {
        producer.addConsumers(of, b);
    }

    public boolean isClosed()
    {
        return producer.isClosed();
    }
}
