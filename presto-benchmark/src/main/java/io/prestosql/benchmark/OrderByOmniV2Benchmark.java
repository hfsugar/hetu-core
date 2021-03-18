package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.operator.*;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.LocalQueryRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.prestosql.spi.block.SortOrder.ASC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;

public class OrderByOmniV2Benchmark
        extends AbstractSimpleOperatorBenchmark
{
    static long buildPageTime = 0;
    static int totalPageCount = 10;
    static int pageDistinctCount = 4;
    static int pageDistinctValueRepeatCount = 250000;

    static Iterator<Page> inputPagesIterator;

    public OrderByOmniV2Benchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "orderby", 0, 10);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories() {
        OperatorFactory tableScanOepratorFactory = createOmniCacheTableScanOperator(
                0,
                new PlanNodeId("test"),
                "orders",
                "orderstatus",
                "totalprice");
        return ImmutableList.of(tableScanOepratorFactory, getOrderByOmniFactory());
    }

    public static void main(String[] args)
    {
        LocalQueryRunner localQueryRunner = createLocalQueryRunner();
        new OrderByOmniV2Benchmark(localQueryRunner).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
        System.out.println("average build page time : " + buildPageTime / 10 + " ms");
    }

    private OrderByOmniOperator.OrderByOmniOperatorFactory getOrderByOmniFactory()
    {
        OrderByOmniOperator.OrderByOmniOperatorFactory operatorFactory = new OrderByOmniOperator.OrderByOmniOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, INTEGER),
                Ints.asList(0, 1),
                Ints.asList(0),
                ImmutableList.of(ASC_NULLS_LAST));
        return operatorFactory;
    }

    private OrderByOperator.OrderByOperatorFactory getOrderByFactory()
    {
        OrderByOperator.OrderByOperatorFactory operatorFactory = new OrderByOperator.OrderByOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(INTEGER, INTEGER),
                Ints.asList(0, 1),
                100_000,
                Ints.asList(0),
                ImmutableList.of(ASC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler());
        return operatorFactory;
    }

    private OperatorFactory createOmniCacheTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");
        return new OperatorFactory() {
            @Override
            public Operator createOperator(DriverContext driverContext) {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                long start = System.currentTimeMillis();
                buildPage();
                buildPageTime += (System.currentTimeMillis() - start);

                ConnectorPageSource pageSource = createOmniCachePageSource();
                return new PageSourceOperator(pageSource, operatorContext);
            }

            @Override
            public void noMoreOperators()
            {
            }

            @Override
            public OperatorFactory duplicate() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public void buildPage()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
            BlockBuilder block1 = pb.getBlockBuilder(0);
            BlockBuilder block2 = pb.getBlockBuilder(1);
            BlockBuilder block3 = pb.getBlockBuilder(2);
            BlockBuilder block4 = pb.getBlockBuilder(3);

            for (int j = 0; j < pageDistinctCount; j++) {
                for (int k = 0; k < pageDistinctValueRepeatCount; k++) {
                    block1.writeLong(i);
                    block2.writeLong(i);
                    block3.writeLong(i);
                    block4.writeLong(i);
                    pb.declarePosition();
                }
            }
            Page page = pb.build();
            inputPages.add(page);
        }
        inputPagesIterator = inputPages.iterator();
    }

    private ConnectorPageSource createOmniCachePageSource()
    {
        return new ConnectorPageSource() {
            boolean isFinished = false;
            @Override
            public long getCompletedBytes() {
                return 0;
            }

            @Override
            public long getReadTimeNanos() {
                return 0;
            }

            @Override
            public boolean isFinished() {
                return isFinished;
            }

            @Override
            public Page getNextPage() {
                if (inputPagesIterator.hasNext()) {
                    Page next = inputPagesIterator.next();
                    return next;
                }
                isFinished = true;
                return null;
            }

            @Override
            public long getSystemMemoryUsage() {
                return 0;
            }

            @Override
            public void close() throws IOException
            {
            }
        };
    }
}
