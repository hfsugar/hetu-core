package io.prestosql.benchmark.shuffle;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.BlockEncodingSerde;
import nova.hetu.ShuffleServiceConfig;
import nova.hetu.shuffle.stream.Stream;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class ShuffleBenchmark
{
    protected static Map<String, String> argsMap = new HashMap<>();
    protected static String ip = "127.0.0.1";
    protected static int port = 12345;
    protected static int totalPages = 1024 * 2;
    protected static int pageSize = 1024 * 1024;
    protected static SerializedPage serializedPage;
    protected static PagesSerde pagesSerde = new BenchmarkPagesSerde();
    protected static Stream.Type streamType = Stream.Type.BASIC;
    protected static String producerId = "task-20210301_92347234_000012_bench.4.1";
    protected static int consumerSize = 10;
    protected static long createPageTimes = 0;
    protected static ShuffleServiceConfig.TransportType transportType = ShuffleServiceConfig.TransportType.UCX;

    private static String DESCRIPTION = "Shuffle Benchmark. \n" +
            "Parameters:\n" +
            "h - print help\n" +
            "ip - ip address to bind sender listener (default: 127.0.0.1) \n" +
            "port - port to bind sender listener (default: 12345)" +

            "num - number of send pages (default: 1024)" +
            "size - size of each page,unit is MB (default: 1)";

    static {
        argsMap.put("ip", "127.0.0.1");
        argsMap.put("port", "12345");
        serializedPage = new SerializedPage(createData(), (byte) 0, 1, createData().length);
    }

    protected static boolean initArgs(String[] args) {
        for (String arg : args) {
            if (arg.contains("h")) {
                System.out.println(DESCRIPTION);
                return false;
            }
            String[] parts = arg.split("=");
            argsMap.put(parts[0], parts[1]);
        }

        if (argsMap.size() == 2) {
            return true;
        }

        try{
            ip = argsMap.get("ip");
            port = Integer.parseInt(argsMap.get("port"));
            totalPages = Integer.parseInt(argsMap.get("num"));
            pageSize = Integer.parseInt(argsMap.get("size")) * 1024 * 1024;
            serializedPage = new SerializedPage(createData(), (byte) 0, 1, pageSize);
        }
        catch (NumberFormatException e) {
            System.out.println(DESCRIPTION);
            return false;
        }

        return true;
    }

    protected static Page createPage()
    {
        long start = System.currentTimeMillis();
        Page page = new Page(createSlicesBlock(createExpectedValues(50)));
        long end = System.currentTimeMillis();
        createPageTimes += (end - start);
        return page;
    }

    protected static URI createURI(int channel)
    {
        URI uri = null;
        try {
            StringBuilder url = new StringBuilder();
            url.append("http://").append(ip).append(":").append(port).append("/v1/task/").append(producerId).append("/results/").append(channel);
            uri = new URI(url.toString());
        }
        catch (Exception e) {
            System.out.println("failed to create URI..." + e.getMessage());
        }
        return uri;
    }

    private static Block createSlicesBlock(Slice[] values)
    {
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 100);

        for (Slice value : values) {
            verify(value != null);
            VARBINARY.writeSlice(builder, value);
        }
        return builder.build();
    }

    private static Slice[] createExpectedValues(int positionCount)
    {
        Slice[] expectedValues = new Slice[positionCount];
        for (int position = 0; position < positionCount; position++) {
            expectedValues[position] = createExpectedValue(position);
        }
        return expectedValues;
    }

    private static Slice createExpectedValue(int length)
    {
        DynamicSliceOutput dynamicSliceOutput = new DynamicSliceOutput(16);
        for (int index = 0; index < length; index++) {
            dynamicSliceOutput.writeByte(length * (index + 1));
        }
        return dynamicSliceOutput.slice();
    }

    private static byte[] createData()
    {
        byte[] data = new byte[pageSize];
        int i = 0;
        while (i < pageSize) {
            data[i] = 'a';
            i++;
        }
        return data;
    }
}

class BenchmarkEncodingSerde
        implements BlockEncodingSerde
{
    @Override
    public Block readBlock(SliceInput input)
    {
        return null;
    }

    @Override
    public void writeBlock(SliceOutput output, Block block)
    {
    }
}

class BenchmarkPagesSerde
        extends PagesSerde
{
    BenchmarkPagesSerde()
    {
        super(new BenchmarkEncodingSerde(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    @Override
    public SerializedPage serialize(Page page)
    {
        return ShuffleBenchmark.serializedPage;
    }

    @Override
    public Page deserialize(SerializedPage serializedPage)
    {
        return ShuffleBenchmark.createPage();
    }
}
