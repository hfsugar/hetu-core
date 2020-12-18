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

import com.google.common.util.concurrent.ListenableFuture;
import io.hetu.core.transport.execution.buffer.PagesSerdeFactory;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nove.hetu.executor.PageProducer;
import nove.hetu.executor.ShuffleService;

import java.util.List;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TaskOutputOperator
        implements Operator
{
    public static class TaskOutputFactory
            implements OutputFactory
    {
        private final OutputBuffer outputBuffer;
        private final List<ShuffleService.Stream> outputStreams;

        public TaskOutputFactory(OutputBuffer outputBuffer, List<ShuffleService.Stream> outputStreams)
        {
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.outputStreams = requireNonNull(outputStreams, "outputStreams is null");
        }

        @Override
        public OperatorFactory createOutputOperator(int operatorId, PlanNodeId planNodeId, List<Type> types, Function<Page, Page> pageLayoutProcessor, PagesSerdeFactory serdeFactory)
        {
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pageLayoutProcessor, outputStreams);
        }
    }

    public static class TaskOutputOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final OutputBuffer outputBuffer;
        private final List<ShuffleService.Stream> outputStreams;
        private final Function<Page, Page> pagePreprocessor;

        public TaskOutputOperatorFactory(int operatorId, PlanNodeId planNodeId, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor, List<ShuffleService.Stream> outputStreams)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
            this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
            this.outputStreams = requireNonNull(outputStreams, "outputStreams is null");
            checkArgument(outputStreams.size() == 1, "there should be only 1 output stream");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, TaskOutputOperator.class.getSimpleName());
            PageProducer pageProducer = new PageProducer(outputStreams.get(0));
            return new TaskOutputOperator(operatorContext, outputBuffer, pagePreprocessor, pageProducer);
        }

        @Override
        public void noMoreOperators()
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TaskOutputOperatorFactory(operatorId, planNodeId, outputBuffer, pagePreprocessor, outputStreams);
        }
    }

    private final OperatorContext operatorContext;
    private final OutputBuffer outputBuffer;
    private final PageProducer pageProducer;
    private final Function<Page, Page> pagePreprocessor;
    private boolean finished;

    public TaskOutputOperator(OperatorContext operatorContext, OutputBuffer outputBuffer, Function<Page, Page> pagePreprocessor, PageProducer pageProducer)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.outputBuffer = requireNonNull(outputBuffer, "outputBuffer is null");
        this.pagePreprocessor = requireNonNull(pagePreprocessor, "pagePreprocessor is null");
        this.pageProducer = requireNonNull(pageProducer, "pageProducer is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        finished = true;
    }

    @Override
    public boolean isFinished()
    {
        if (finished && isBlocked().isDone()) {
            System.out.println("Operator " + operatorContext.getOperatorId() + " processed " + operatorContext.getInputPositions().getTotalCount() + " rows");
        }
        return finished && isBlocked().isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        ListenableFuture<?> blocked = outputBuffer.isFull();
        return blocked.isDone() ? NOT_BLOCKED : blocked;
    }

    @Override
    public boolean needsInput()
    {
        return !finished && isBlocked().isDone();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        if (page.getPositionCount() == 0) {
            return;
        }

        page = pagePreprocessor.apply(page);

        if (true /** grpc.enabled = true */) {
            //redirect to grpc shuffle service
            try {
                pageProducer.send(page);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
        else {
            outputBuffer.enqueue(page);
        }
        operatorContext.recordOutput(page.getSizeInBytes(), page.getPositionCount());
    }

    @Override
    public Page getOutput()
    {
        return null;
    }
}
