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
import io.prestosql.metadata.Metadata;
import io.prestosql.operator.DriverContext;
import io.prestosql.operator.OrderByOmniOperator;
import io.prestosql.operator.OrderByOperator;
import io.prestosql.operator.PagesIndex;
import io.prestosql.spi.Page;
import io.prestosql.sql.gen.JoinCompiler;
import io.prestosql.sql.gen.OrderingCompiler;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
import org.jetbrains.annotations.NotNull;
import tool.RowPagesBuilder;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.succinctBytes;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.block.SortOrder.DESC_NULLS_LAST;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static tool.OperatorAssertion.toPages;
import static tool.RowPagesBuilder.rowPagesBuilder;
import static tool.SessionTestUtils.TEST_SESSION;

public class OrderByBench
{
    private static final Metadata metadata = createTestMetadataManager();

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(createTestMetadataManager());

    int pageDistinctCount = 4;
    int pageDistinctValueRepeatCount = 250;
    int totalPageCount = 2000;
    int threadNum = 10;
    static AtomicLong totalTime = new AtomicLong(0);
    public boolean useOmni;

    public static void main(String[] args)
    {
        OrderByBench orderByBench = new OrderByBench();
        orderByBench.setUp();
        if (args[0].equals("omni")) {
            orderByBench.useOmni = true;
        }

        for (int i = 1; i < args.length; i++) {
            orderByBench.threadNum = Integer.valueOf(args[i]);
            orderByBench.testOrderBy();
            System.out.println("use omni:" + orderByBench.useOmni + " " + orderByBench.threadNum + " threads average take:" + (totalTime.get() / orderByBench.threadNum) + " ms");
        }

        orderByBench.tearDown();
    }

    public void testOrderBy()
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 2_000_000;
        int pageCount = 2000;
        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult expected = getExpectedMaterializedRows(numberOfRows, driverContext);

        CountDownLatch countDownLatch = new CountDownLatch(threadNum);
        CopyOnWriteArrayList<List<Page>> resultList = new CopyOnWriteArrayList<>();
        for (int i = 0; i < threadNum; i++) {
            int id = i;
            Thread thread = new Thread(() -> {
                try {
                    RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT, BIGINT);
                    for (int j = 0; j < pageCount; j++) {
                        rowPagesBuilder.addSequencePage(numberOfRows / pageCount, 0, 0);
                    }
                    List<Page> input = rowPagesBuilder.build();

                    long start = System.currentTimeMillis();
                    List<Page> pages;
                    if (useOmni) {
                        pages = toPages(getOrderByOmniOperatorFactory(id), driverContext, input, false);
                    }
                    else {
                        pages = toPages(getOriginalOrderByFactory(id), driverContext, input, false);
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
//            assertEquals(toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages), expected.getMaterializedRows());
        }
        System.out.println("Threads: " + threadNum + " OrderBy benchmark finished");
    }

    @NotNull
    private OrderByOmniOperator.OrderByOmniOperatorFactory getOrderByOmniOperatorFactory(int id)
    {
        OrderByOmniOperator.OrderByOmniOperatorFactory operatorFactory = new OrderByOmniOperator.OrderByOmniOperatorFactory(
                id,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(1),
                ImmutableList.of(0, 1),
                ImmutableList.of(DESC_NULLS_LAST, DESC_NULLS_LAST));
        return operatorFactory;
    }

    private OrderByOperator.OrderByOperatorFactory getOriginalOrderByFactory(int id)
    {
        OrderByOperator.OrderByOperatorFactory operatorFactory = new OrderByOperator.OrderByOperatorFactory(
                id,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(1),
                10,
                ImmutableList.of(0),
                ImmutableList.of(DESC_NULLS_LAST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler());
        return operatorFactory;
    }

    private MaterializedResult getExpectedMaterializedRows(int numberOfRows, DriverContext driverContext)
    {
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((long) (numberOfRows - i - 1));
        }
        MaterializedResult expected = expectedBuilder.build();
        return expected;
    }

    protected static final JoinCompiler JOIN_COMPILER = new JoinCompiler(createTestMetadataManager());

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
