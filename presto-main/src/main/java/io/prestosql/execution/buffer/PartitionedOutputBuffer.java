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
package io.prestosql.execution.buffer;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.memory.context.LocalMemoryContext;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.PARTITIONED;
import static java.util.Objects.requireNonNull;

public class PartitionedOutputBuffer
        extends OutputBuffer
{
    private final OutputBuffers outputBuffers;
    private final List<ClientBuffer> partitions;

    public PartitionedOutputBuffer(
            java.lang.String taskInstanceId,
            StateMachine<BufferState> state,
            OutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor,
            PagesSerde serde)
    {
        super(state, maxBufferSize, systemMemoryContextSupplier, notificationExecutor, serde);
        requireNonNull(outputBuffers, "outputBuffers is null");
        checkArgument(outputBuffers.getType() == PARTITIONED, "Expected a PARTITIONED output buffer descriptor");
        checkArgument(outputBuffers.isNoMoreBufferIds(), "Expected a final output buffer descriptor");
        this.outputBuffers = outputBuffers;

        ImmutableList.Builder<ClientBuffer> partitions = ImmutableList.builder();
        for (String bufferId : outputBuffers.getBuffers().keySet()) {
            ClientBuffer partition = new ClientBuffer(taskInstanceId, bufferId);
            partitions.add(partition);
        }
        this.partitions = partitions.build();

        state.compareAndSet(OPEN, NO_MORE_BUFFERS);
        state.compareAndSet(NO_MORE_PAGES, FLUSHING);
        checkFlushComplete();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public OutputBufferStatistics getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        int totalBufferedPages = 0;
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer partition : partitions) {
            BufferInfo bufferInfo = partition.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferStatistics(
                "PARTITIONED",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers, PagesSerde serde)
    {
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        // ignore buffers added after query finishes, which can happen when a query is canceled
        // also ignore old versions, which is normal
        if (state.get().isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
            return;
        }

        // no more buffers can be added but verify this is valid state change
        outputBuffers.checkValidTransition(newOutputBuffers);
    }

    void doEnqueue(int partitionNumber, List<SerializedPage> pages)
    {
        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(bufferedPage -> new SerializedPageReference(bufferedPage, 1, () -> memoryManager.updateMemoryUsage(-bufferedPage.getRetainedSizeInBytes())))
                .collect(toImmutableList());

        // add pages to the buffer (this will increase the reference count by one)
        partitions.get(partitionNumber).enqueuePages(serializedPageReferences);

        // drop the initial reference
        serializedPageReferences.forEach(SerializedPageReference::dereferencePage);
    }

    @Override
    public ListenableFuture<BufferResult> get(String outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return partitions.get(Integer.valueOf(outputBufferId)).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(String outputBufferId, long sequenceId)
    {
        requireNonNull(outputBufferId, "bufferId is null");

        partitions.get(Integer.valueOf(outputBufferId)).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(String bufferId)
    {
        requireNonNull(bufferId, "bufferId is null");

        partitions.get(Integer.valueOf(bufferId)).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        partitions.forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            partitions.forEach(ClientBuffer::destroy);
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
        }
    }

    @Override
    public void fail()
    {
        // ignore fail if the buffer already in a terminal state.
        if (state.setIf(FAILED, oldState -> !oldState.isTerminal())) {
            memoryManager.setNoBlockOnFull();
            forceFreeMemory();
            // DO NOT destroy buffers or set no more pages.  The coordinator manages the teardown of failed queries.
        }
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING && state.get() != NO_MORE_BUFFERS) {
            return;
        }

        if (partitions.stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }
}
