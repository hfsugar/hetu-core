package operator;/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.HashAggregationOmniOperatorV2;
import io.prestosql.operator.HashAggregationOperator;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Signature;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import nova.hetu.omnicache.vector.AggType;
import nova.hetu.omnicache.vector.VecType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static tool.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static tool.OperatorAssertion.toPages;
import static tool.SessionTestUtils.TEST_SESSION;

public class HashAggBench
{
    private static final Metadata metadata = createTestMetadataManager();

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());

    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 250 * 32;
    int totalPageCount = 200;
    int threadNum = 10;
    static AtomicLong totalTime = new AtomicLong(0);
    public boolean useOmni;

    public static void main(String[] args)
    {
        HashAggBench hashAggBench = new HashAggBench();
        hashAggBench.setUp();
        if (args[0].equals("omni")) {
            hashAggBench.useOmni = true;
        }

        for (int i = 1; i < args.length; i++) {
            hashAggBench.threadNum = Integer.valueOf(args[i]);
            hashAggBench.testHashAggregation();
            System.out.println("use omni:" + hashAggBench.useOmni + "; " + hashAggBench.threadNum + " threads average take:" + (totalTime.get() / hashAggBench.threadNum) + " ms");
            System.out.println();
        }

        hashAggBench.tearDown();
    }

    public void testHashAggregation()
    {
        int omniTotalChannels = 4;
        int[] omniGrouByChannels = {0, 1};
        VecType[] omniGroupByTypes = {VecType.LONG, VecType.LONG};
        int[] omniAggregationChannels = {2, 3};
        VecType[] omniAggregationTypes = {VecType.LONG, VecType.LONG};
        AggType[] omniAggregator = {AggType.SUM, AggType.SUM};
        VecType[] omniAggReturnTypes = {VecType.LONG, VecType.LONG};
        List<VecType[]> inAndOutputTypes = new ArrayList<>();
        inAndOutputTypes.add(new VecType[] {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG});
        inAndOutputTypes.add(new VecType[] {VecType.LONG, VecType.LONG, VecType.LONG, VecType.LONG});
        int[] outputLayout = new int[] {0, 1, 2, 3};

//        expected
        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult expected = getExpectedMaterializedRows(driverContext);

        long stageID = UUID.randomUUID().getMostSignificantBits() & Long.MAX_VALUE;

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        CopyOnWriteArrayList<List<Page>> resultList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    List<Page> input = builderPage();
                    long start = System.currentTimeMillis();
                    List<Page> pages;
                    if (useOmni) {
                        pages = toPages(new HashAggregationOmniOperatorV2.HashAggregationOmniOperatorFactory(Optional.empty(), id, new PlanNodeId(String.valueOf(id)), stageID, omniTotalChannels, omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, omniAggReturnTypes, inAndOutputTypes, outputLayout), driverContext, input, false);
                    }
                    else {
                        pages = toPages(getOriginalAggFactory(id), driverContext, input, false);
                    }
                    long end = System.currentTimeMillis();
                    totalTime.addAndGet(end - start);
                    resultList.add(pages);
                }
                finally {
                    countDownLatch.countDown();
                }
            });
            thread.start();
        }
        try {
            countDownLatch.await();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }

        assertEquals(resultList.size(), threadNum);
        for (List<Page> pages : resultList) {
            assertPagesEqualIgnoreOrder(driverContext, pages, expected, false, Optional.empty());
        }
        System.out.println("Threads: " + threadNum + " Hash Agg benchmark finished");
    }

    private MaterializedResult getExpectedMaterializedRows(DriverContext driverContext)
    {
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT);
        long sum = totalPageCount * pageDistinctValueRepeatCount;
        for (int i = 0; i < pageDistinctCount; i++) {
            expectedBuilder.row((long) i, (long) i, sum, sum);
        }
        MaterializedResult expected = expectedBuilder.build();
        return expected;
    }

    private List<Page> builderPage()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);

        List<Page> inputPages = new ArrayList<>();
        for (int k = 0; k < totalPageCount; k++) {
            PageBuilder pb = PageBuilder.withMaxPageSize(Integer.MAX_VALUE, dataTypes);
            BlockBuilder group1 = pb.getBlockBuilder(0);
            BlockBuilder group2 = pb.getBlockBuilder(1);
            BlockBuilder sum1 = pb.getBlockBuilder(2);
            BlockBuilder sum2 = pb.getBlockBuilder(3);

            for (int i = 0; i < pageDistinctCount; i++) {
                for (int j = 0; j < pageDistinctValueRepeatCount; j++) {
                    group1.writeLong(i);
                    group2.writeLong(i);
                    sum1.writeLong(1);
                    sum2.writeLong(1);
                    pb.declarePosition();
                }
            }
            Page build = pb.build();

            inputPages.add(build);
        }
        return inputPages;
    }

    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler(createTestMetadataManager());

    private HashAggregationOperator.HashAggregationOperatorFactory getOriginalAggFactory(int id)
    {
        InternalAggregationFunction bigintSum = metadata.getAggregateFunctionImplementation(
                new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        HashAggregationOperator.HashAggregationOperatorFactory aggregationOperatorFactory = new HashAggregationOperator.HashAggregationOperatorFactory(
                id,
                new PlanNodeId(String.valueOf(id)),
                ImmutableList.of(BIGINT, BIGINT),
                Ints.asList(0, 1),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                ImmutableList.of(bigintSum.bind(ImmutableList.of(2), Optional.empty()), bigintSum.bind(ImmutableList.of(3), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                JOIN_COMPILER,
                false);
        return aggregationOperatorFactory;
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .setQueryMaxMemory(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }
}
