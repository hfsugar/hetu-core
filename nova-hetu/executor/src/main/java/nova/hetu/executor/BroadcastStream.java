package nova.hetu.executor;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class BroadcastStream
        extends Stream
{
    PagesSerde serde;
    Map<Integer, BlockingQueue<SerializedPage>> channels = new HashMap<>();

    public BroadcastStream(String id, PagesSerde serde)
    {
        super(id, serde);
        this.serde = serde;
    }

    @Override
    public void write(Page page)
    {
        for (BlockingQueue<SerializedPage> queue : channels.values()) {
            queue.offer(serde.serialize(page));
        }
    }

    public SerializedPage take(int channelId)
    {
        return channels.get(channelId).poll();
    }

    public void addChannels(List<Integer> channelIds)
    {
        for (int channelId : channelIds) {
            channels.put(channelId, new LinkedBlockingQueue<>());
        }
    }
}
