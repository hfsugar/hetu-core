/*
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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import io.airlift.units.DataSize;
import io.prestosql.metadata.Metadata;
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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static io.prestosql.operator.OperatorAssertion.toPages;
import static io.prestosql.spi.function.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHashAggregationOmniOperatorV2
{
    private static final Metadata metadata = createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getAggregateFunctionImplementation(
            new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = metadata.getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getAggregateFunctionImplementation(
            new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private List<Page> builderPage()
    {
        List<Type> dataTypes = new ArrayList<>();
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
        dataTypes.add(BIGINT);
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

        List<Page> inputPages = new ArrayList<>();
        for (int i = 0; i < totalPageCount; i++) {
            inputPages.add(build);
        }
        return inputPages;
    }

    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 250000;
    int totalPageCount = 10;

    @Test(invocationCount = 1)
    public void testHashAggregation()
    {
//        try {
//            Thread.sleep(20000);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
        int threadNum = 1;
        List<Page> input = builderPage();

        int omniTotalChannels = 4;
        int[] omniGrouByChannels = {0, 1};
        VecType[] omniGroupByTypes = {VecType.LONG, VecType.LONG};
        int[] omniAggregationChannels = {2, 3};
        VecType[] omniAggregationTypes = {VecType.LONG, VecType.LONG};
        AggType[] omniAggregator = {AggType.SUM, AggType.SUM};
        VecType[] omniAggReturnTypes = {VecType.LONG, VecType.LONG};

//        expected
        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT, BIGINT);
        long sum = totalPageCount * pageDistinctValueRepeatCount;
        for (int i = 0; i < pageDistinctCount; i++) {
            expectedBuilder.row((long) i, (long) i, sum, sum);
        }
        MaterializedResult expected = expectedBuilder.build();

        ExecutorService service = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(threadNum));
        ArrayList<ListenableFuture<List<Page>>> futureArrayList = new ArrayList<>();
        List<List<Page>> pagesList = new ArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int id = i + 1;

            HashAggregationOmniOperatorV2.HashAggregationOmniOperatorFactory hashAggregationOmniV2OperatorFactory = new HashAggregationOmniOperatorV2.HashAggregationOmniOperatorFactory(id, new PlanNodeId(String.valueOf(id)), omniTotalChannels, omniGrouByChannels, omniGroupByTypes, omniAggregationChannels, omniAggregationTypes, omniAggregator, omniAggReturnTypes);

            ListenableFuture<List<Page>> submit = (ListenableFuture<List<Page>>) service.submit(() -> toPages(hashAggregationOmniV2OperatorFactory, driverContext, input, false));
//            ListenableFuture<List<Page>> submit = (ListenableFuture<List<Page>>) service.submit(() -> toPages(getOriginalAggFactory(), driverContext, input, false));
            futureArrayList.add(submit);
        }

        waitAllTaskFinished(futureArrayList, pagesList);

        assertEquals(pagesList.size(), threadNum);
        for (int i = 0; i < pagesList.size(); i++) {
            assertPagesEqualIgnoreOrder(driverContext, pagesList.get(i), expected, false, Optional.empty());
        }
//        System.out.println("all finished");
//        try {
//            Thread.sleep(10000);
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    private void waitAllTaskFinished(ArrayList<ListenableFuture<List<Page>>> futureArrayList, List<List<Page>> pagesList)
    {
        while (!futureArrayList.isEmpty()) {
            Iterator<ListenableFuture<List<Page>>> iterator = futureArrayList.iterator();
            while (iterator.hasNext()) {
                ListenableFuture<List<Page>> next = iterator.next();
                if (next.isDone()) {
                    if (futureArrayList.size() % 10 == 0) {
                        System.out.println("thread i finsished: " + futureArrayList.size());
                    }
                    try {
                        List<Page> pages = next.get();
                        pagesList.add(pages);
                    }
                    catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    catch (ExecutionException e) {
                        e.printStackTrace();
                    }
                    iterator.remove();
                }
            }
        }
    }

    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler(createTestMetadataManager());

    private HashAggregationOperator.HashAggregationOperatorFactory getOriginalAggFactory()
    {
        InternalAggregationFunction doubleSum = metadata.getAggregateFunctionImplementation(
                new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
        HashAggregationOperator.HashAggregationOperatorFactory aggregationOperatorFactory = new HashAggregationOperator.HashAggregationOperatorFactory(
                1,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, BIGINT),
                Ints.asList(0, 1),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                ImmutableList.of(doubleSum.bind(ImmutableList.of(2), Optional.empty()), doubleSum.bind(ImmutableList.of(3), Optional.empty())),
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
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
