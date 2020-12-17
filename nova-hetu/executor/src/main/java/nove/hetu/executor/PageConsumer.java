package nove.hetu.executor;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class PageConsumer
{
    LinkedBlockingQueue<SerializedPage> pageOutputBuffer;
    PagesSerde serde;
    Future future;
    public PageConsumer(String taskid, String bufferid, PagesSerde serde) {
        this.pageOutputBuffer = new LinkedBlockingQueue<SerializedPage>();
        this.serde = serde;
        future = ShuffleClient.getResults("127.0.0.1", 16544, taskid, bufferid, pageOutputBuffer);
    }

    public Page take()
            throws InterruptedException
    {
        return serde.deserialize(pageOutputBuffer.take());
    }

    public boolean ended() {
        return pageOutputBuffer.isEmpty() && future.isDone();
    }
}
