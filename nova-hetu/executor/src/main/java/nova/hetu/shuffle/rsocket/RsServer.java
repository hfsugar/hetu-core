package nova.hetu.shuffle.rsocket;

import io.rsocket.core.RSocketServer;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.transport.netty.server.TcpServerTransport;

public class RsServer
{
    public static void main(String[] args)
            throws InterruptedException
    {
        start();
    }

    public static void start()
            throws InterruptedException
    {
        RSocketServer.create(new PageHandler())
                .payloadDecoder(PayloadDecoder.ZERO_COPY)
                .bind(TcpServerTransport.create(7878))
                .block()
                .onClose();

        System.out.println("Server started");
    }

    public static void shutdown()
    {
        //do nothing for now
    }
}
