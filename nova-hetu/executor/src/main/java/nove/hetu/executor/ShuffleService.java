/*
 * Copyright (C) 2018-2020. Huawei Technologies Co., Ltd. All rights reserved.
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
package nove.hetu.executor;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.units.DataSize;
import io.grpc.stub.StreamObserver;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskManager;
import io.prestosql.execution.buffer.BufferResult;
import io.prestosql.execution.buffer.OutputBuffers;
import io.prestosql.metadata.SessionPropertyManager;
import nova.hetu.executor.ExecutorOuterClass;
import nova.hetu.executor.ShuffleGrpc;

import java.util.concurrent.ScheduledExecutorService;

public class ShuffleService
        extends ShuffleGrpc.ShuffleImplBase
{
    private TaskManager taskManager;
    private SessionPropertyManager sessionPropertyManager;
    BoundedExecutor responseExecutor;
    ScheduledExecutorService timeoutExecutor;

    @Inject
    public ShuffleService(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            BoundedExecutor responseExecutor,
            ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = taskManager;
        this.sessionPropertyManager = sessionPropertyManager;
    }

    @Override
    public void getResult(ExecutorOuterClass.Task request, StreamObserver<ExecutorOuterClass.Page> responseObserver)
    {
        TaskId taskId = TaskId.valueOf(request.getTaskId());
        OutputBuffers.OutputBufferId outputBufferId = OutputBuffers.OutputBufferId.fromString(request.getBufferId());
        DataSize max = DataSize.valueOf(request.getMaxSize());
        long token = request.getToken();
        long start = System.nanoTime();
        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, outputBufferId, token, max);
    }
}
