package nova.hetu.executor;

import io.hetu.core.transport.execution.buffer.PagesSerde;
import nova.hetu.executor.PageProducer.Type;

import static nova.hetu.executor.PageProducer.Type.BROADCAST;

public class StreamFactory
{
    public Stream create(String producerId, PagesSerde pagesSerde, Type type)
    {
        if (type == BROADCAST) {
            return new BroadcastStream(producerId, pagesSerde);
        }
        else {
            return new Stream(producerId, pagesSerde);
        }
    }
}
