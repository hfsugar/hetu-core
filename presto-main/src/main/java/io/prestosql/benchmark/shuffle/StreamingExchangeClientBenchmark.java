package io.prestosql.benchmark.shuffle;

import io.airlift.units.DataSize;
import io.prestosql.memory.context.AggregatedMemoryContext;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.memory.context.SimpleLocalMemoryContext;
import io.prestosql.operator.StreamingExchangeClient;
import io.prestosql.spi.Page;
import nova.hetu.ShuffleServiceConfig;

import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newScheduledThreadPool;

public class StreamingExchangeClientBenchmark extends ShuffleBenchmark
{
    private static DataSize maxBufferSize = new DataSize(32, DataSize.Unit.MEGABYTE);
    private static LocalMemoryContext localMemoryContext = new SimpleLocalMemoryContext(AggregatedMemoryContext.newSimpleAggregatedMemoryContext(), "simpleTag");
    private static ScheduledExecutorService schedule = newScheduledThreadPool(25, daemonThreadsNamed("exchange-client-%s"));

    public static void main(String[] args)
    {
        StreamingExchangeClient client = new StreamingExchangeClient(maxBufferSize,
                10,
                localMemoryContext,
                pagesSerde,
                transportType,
                1024 * 1024,
                64,
                false,
                schedule);

        int i = 0;
        while (i < consumerSize) {
            client.addLocation(createURI(i));
            i++;
        }
        client.setNoMoreLocation();

        int recvPage = 0;
        int tmp = 1;

        long start = System.currentTimeMillis();
        try {
            while (!client.isFinished()) {
                Page page = client.pollPage();
                if (page != null) {
                    recvPage++;
                }

                if (tmp < recvPage && recvPage % 1024 == 0) {
                    tmp = recvPage + 1;
                    System.out.println(String.format("client already receive pages number = %s", recvPage));
                }
            }
        }
        catch (Exception e) {
            System.out.println("error happened.. cause: " + e.getMessage());
        }
        long end = System.currentTimeMillis();
        client.close();
        System.out.println(String.format("success to get all pages. page size=%s, recv times=%s ms, create pages time=%s ms", recvPage, (end - start), createPageTimes));
    }
}
