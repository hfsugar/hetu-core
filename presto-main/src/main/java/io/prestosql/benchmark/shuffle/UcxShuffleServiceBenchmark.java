package io.prestosql.benchmark.shuffle;

import nova.hetu.ShuffleServiceConfig;
import nova.hetu.UcxServer;
import nova.hetu.shuffle.PageProducer;

import java.util.ArrayList;
import java.util.List;

public class UcxShuffleServiceBenchmark extends ShuffleBenchmark
{
    public static void main(String[] args) throws Exception
    {
        if (!initArgs(args)) {
            return;
        }

        ShuffleServiceConfig config = new ShuffleServiceConfig();
        config.setHost(ip);
        config.setPort(port);

        UcxServer server = new UcxServer(config);
        server.start();

        PageProducer pageProducer = new PageProducer(producerId, pagesSerde, streamType, 1024* 1024);

        List<Integer> newConsumers = new ArrayList<>();
        int i = 0;
        while (i < consumerSize) {
            newConsumers.add(i);
            i++;
        }
        pageProducer.addConsumers(newConsumers, true);

        i = 0;
        while (i < totalPages) {
            pageProducer.send(createPage());
            i++;
        }
        System.out.println("success to send " + totalPages + " pages to Stream[" + producerId + "]");

        pageProducer.close();
    }
}
