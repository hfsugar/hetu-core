package nove.hetu.executor;

import io.prestosql.spi.Page;

public class PageProducer
{
    private ShuffleService.Stream out;

    public PageProducer(ShuffleService.Stream out) {
        this.out = out;
    }

    public void send(Page page)
            throws InterruptedException
    {
        out.write(page);
    }
}
