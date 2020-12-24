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
package io.prestosql.execution;

import io.airlift.concurrent.SetThreadName;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.prestosql.Session;
import io.prestosql.event.SplitMonitor;
import io.prestosql.execution.buffer.OutputBuffer;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.execution.executor.TaskExecutor;
import io.prestosql.memory.QueryContext;
import io.prestosql.operator.TaskContext;
import io.prestosql.sql.planner.LocalExecutionPlanner;
import io.prestosql.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import io.prestosql.sql.planner.PlanFragment;
import io.prestosql.sql.planner.TypeProvider;
import nova.hetu.shuffle.PageProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import java.util.concurrent.Executor;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.prestosql.execution.SqlTaskExecution.createSqlTaskExecution;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static java.util.Objects.requireNonNull;

public class SqlTaskExecutionFactory
{
    private final Executor taskNotificationExecutor;

    private final TaskExecutor taskExecutor;

    private final LocalExecutionPlanner planner;
    private final SplitMonitor splitMonitor;
    private final boolean perOperatorCpuTimerEnabled;
    private final boolean cpuTimerEnabled;

    public SqlTaskExecutionFactory(
            Executor taskNotificationExecutor,
            TaskExecutor taskExecutor,
            LocalExecutionPlanner planner,
            SplitMonitor splitMonitor,
            TaskManagerConfig config)
    {
        this.taskNotificationExecutor = requireNonNull(taskNotificationExecutor, "taskNotificationExecutor is null");
        this.taskExecutor = requireNonNull(taskExecutor, "taskExecutor is null");
        this.planner = requireNonNull(planner, "planner is null");
        this.splitMonitor = requireNonNull(splitMonitor, "splitMonitor is null");
        requireNonNull(config, "config is null");
        this.perOperatorCpuTimerEnabled = config.isPerOperatorCpuTimerEnabled();
        this.cpuTimerEnabled = config.isTaskCpuTimerEnabled();
    }

    public SqlTaskExecution create(Session session, QueryContext queryContext, TaskStateMachine taskStateMachine, OutputBuffer outputBuffer, PlanFragment fragment, List<TaskSource> sources, OptionalInt totalPartitions, PagesSerde pagesSerde, OutputBuffers outputBuffers)
    {
        TaskContext taskContext = queryContext.addTaskContext(
                taskStateMachine,
                session,
                perOperatorCpuTimerEnabled,
                cpuTimerEnabled,
                totalPartitions);
        List<PageProducer> producers = createProducers(taskStateMachine.getTaskId(), totalPartitions, pagesSerde, outputBuffers.getType());

        LocalExecutionPlan localExecutionPlan;
        try (SetThreadName ignored = new SetThreadName("Task-%s", taskStateMachine.getTaskId())) {
            try {
                localExecutionPlan = planner.plan(
                        taskContext,
                        fragment.getRoot(),
                        TypeProvider.copyOf(fragment.getSymbols()),
                        fragment.getPartitioningScheme(),
                        fragment.getStageExecutionDescriptor(),
                        fragment.getPartitionedSources(),
                        outputBuffer,
                        producers);
            }
            catch (Throwable e) {
                // planning failed
                taskStateMachine.failed(e);
                throwIfUnchecked(e);
                throw new RuntimeException(e);
            }
        }
        return createSqlTaskExecution(
                taskStateMachine,
                taskContext,
                outputBuffer,
                producers,
                sources,
                localExecutionPlan,
                taskExecutor,
                taskNotificationExecutor,
                splitMonitor);
    }

    private List<PageProducer> createProducers(TaskId taskId, OptionalInt totalPartitions, PagesSerde pagesSerde, OutputBuffers.BufferType type)
    {
        List<PageProducer> producers = new ArrayList<>();
        int numPartitions = totalPartitions.orElse(1); // TODO: check when can this be empty
        if (type == PARTITIONED) {
            for (int partition = 0; partition < numPartitions; partition++) { //partition id is 0 based
                producers.add(PageProducer.create(getProducerId(taskId.toString(), partition), pagesSerde, PageProducer.Type.PARTITIONED));
            }
        }
        else if (type == BROADCAST) {
            producers.add(PageProducer.create(taskId.toString(), pagesSerde, PageProducer.Type.BROADCAST));
        }
        else {
            producers.add(PageProducer.create(getProducerId(taskId.toString(), 0), pagesSerde, PageProducer.Type.ARBITRARY));
        }

        return producers;
    }

    private static String getProducerId(String taskId, int partitionId)
    {
        return String.format("%s-%d", taskId, partitionId);
    }
}
