package nova.hetu.executor;

import io.hetu.core.transport.execution.buffer.PageCodecMarker;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

import static io.airlift.slice.Slices.EMPTY_SLICE;

/**
 * must be used in a the following way to ensure proper handling of releasing the resources
 * try (Out out = ShuffleService.getOutStream(task)) {
 * out.write(page);
 * }
 */
class Stream
        implements AutoCloseable
{
    static final SerializedPage EOS = new SerializedPage(EMPTY_SLICE, PageCodecMarker.MarkerSet.empty(), 0, 0);
    private final PagesSerde serde;
    ArrayBlockingQueue<SerializedPage> queue = new ArrayBlockingQueue(100 /** shuffle.grpc.buffer_size_in_item */);
    String id;
    boolean eos; // endOfStream
    boolean noMoreChannels;

    Stream(String id, PagesSerde serde)
    {
        this.id = id;
        this.serde = serde;
    }

    SerializedPage take()
            throws InterruptedException
    {
        return queue.take();
    }

    /**
     * write out the page synchronously
     *
     * @param page
     */
    public void write(Page page)
            throws InterruptedException
    {
        if (eos) {
            throw new IllegalStateException("Output stream is closed already");
        }
        queue.put(serde.serialize(page));
    }

    public void addChannels(List<Integer> channelIds)
    {

    }

    public void setNoMoreChannels()
    {
        noMoreChannels = true;
    }

    public boolean isClosed()
    {
        return eos && queue.isEmpty();
    }

    @Override
    public void close()
            throws Exception
    {
        eos = true;
        queue.put(EOS);
    }
}
