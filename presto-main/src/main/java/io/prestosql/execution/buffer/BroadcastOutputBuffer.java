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
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.memory.context.LocalMemoryContext;

import javax.annotation.concurrent.GuardedBy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static java.util.Objects.requireNonNull;

public class BroadcastOutputBuffer
        extends OutputBuffer
{
    private final java.lang.String taskInstanceId;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = OutputBuffers.createInitialEmptyOutputBuffers(BROADCAST);

    @GuardedBy("this")
    private final Map<String, ClientBuffer> buffers = new ConcurrentHashMap<>();

    @GuardedBy("this")
    private final List<SerializedPageReference> initialPagesForNewBuffers = new ArrayList<>();

    public BroadcastOutputBuffer(
            java.lang.String taskInstanceId,
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor,
            PagesSerde serde)
    {
        super(state, maxBufferSize, systemMemoryContextSupplier, notificationExecutor, serde);
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
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

        // buffer it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        return new OutputBufferStatistics(
                "BROADCAST",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages.get(),
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                buffers.stream()
                        .map(ClientBuffer::getInfo)
                        .collect(toImmutableList()));
    }

    @Override
    public void setOutputBuffers(OutputBuffers newOutputBuffers, PagesSerde serde)
    {
        checkState(!Thread.holdsLock(this), "Can not set output buffers while holding a lock on this");
        requireNonNull(newOutputBuffers, "newOutputBuffers is null");

        synchronized (this) {
            // ignore buffers added after query finishes, which can happen when a query is canceled
            // also ignore old versions, which is normal
            BufferState state = this.state.get();
            if (state.isTerminal() || outputBuffers.getVersion() >= newOutputBuffers.getVersion()) {
                return;
            }

            // verify this is valid state change
            outputBuffers.checkValidTransition(newOutputBuffers);
            outputBuffers = newOutputBuffers;

            // add the new buffers
            for (Entry<String, Integer> entry : outputBuffers.getBuffers().entrySet()) {
                if (!buffers.containsKey(entry.getKey())) {
                    ClientBuffer buffer = getBuffer(entry.getKey());
                    if (!state.canAddPages()) {
                        buffer.setNoMorePages();
                    }
                }
            }

            // update state if no more buffers is set
            if (outputBuffers.isNoMoreBufferIds()) {
                this.state.compareAndSet(OPEN, NO_MORE_BUFFERS);
                this.state.compareAndSet(NO_MORE_PAGES, FLUSHING);
            }
        }

        if (!state.get().canAddBuffers()) {
            noMoreBuffers();
        }

        checkFlushComplete();
    }

    void doEnqueue(int part, List<SerializedPage> pages)
    {
        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(pageSplit -> new SerializedPageReference(pageSplit, 1, () -> {
                    checkState(totalBufferedPages.decrementAndGet() >= 0);
                    memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes());
                }))
                .collect(toImmutableList());

        // if we can still add buffers, remember the pages for the future buffers
        Collection<ClientBuffer> buffers;
        synchronized (this) {
            if (state.get().canAddBuffers()) {
                serializedPageReferences.forEach(SerializedPageReference::addReference);
                initialPagesForNewBuffers.addAll(serializedPageReferences);
            }

            // make a copy while holding the lock to avoid race with initialPagesForNewBuffers.addAll above
            buffers = safeGetBuffersSnapshot();
        }

        // add pages to all existing buffers (each buffer will increment the reference count)
        buffers.forEach(partition -> partition.enqueuePages(serializedPageReferences));

        // drop the initial reference
        serializedPageReferences.forEach(SerializedPageReference::dereferencePage);
    }

    @Override
    public ListenableFuture<BufferResult> get(String outputBufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not get pages while holding a lock on this");
        requireNonNull(outputBufferId, "outputBufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(outputBufferId).getPages(startingSequenceId, maxSize);
    }

    @Override
    public void acknowledge(String bufferId, long sequenceId)
    {
        checkState(!Thread.holdsLock(this), "Can not acknowledge pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).acknowledgePages(sequenceId);
    }

    @Override
    public void abort(String bufferId)
    {
        checkState(!Thread.holdsLock(this), "Can not abort while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");

        getBuffer(bufferId).destroy();

        checkFlushComplete();
    }

    @Override
    public void setNoMorePages()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more pages while holding a lock on this");
        state.compareAndSet(OPEN, NO_MORE_PAGES);
        state.compareAndSet(NO_MORE_BUFFERS, FLUSHING);
        memoryManager.setNoBlockOnFull();

        safeGetBuffersSnapshot().forEach(ClientBuffer::setNoMorePages);

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Can not destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            safeGetBuffersSnapshot().forEach(ClientBuffer::destroy);

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

    private synchronized ClientBuffer getBuffer(String id)
    {
        ClientBuffer buffer = buffers.get(id);
        if (buffer != null) {
            return buffer;
        }

        // NOTE: buffers are allowed to be created in the FINISHED state because destroy() can move to the finished state
        // without a clean "no-more-buffers" message from the scheduler.  This happens with limit queries and is ok because
        // the buffer will be immediately destroyed.
        BufferState state = this.state.get();
        checkState(state.canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id);

        // do not setup the new buffer if we are already failed
        if (state != FAILED) {
            // add initial pages
            buffer.enqueuePages(initialPagesForNewBuffers);

            // update state
            if (!state.canAddPages()) {
                // BE CAREFUL: set no more pages only if not FAILED, because this allows clients to FINISH
                buffer.setNoMorePages();
            }

            // buffer may have finished immediately before calling this method
            if (state == FINISHED) {
                buffer.destroy();
            }
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private void noMoreBuffers()
    {
        checkState(!Thread.holdsLock(this), "Can not set no more buffers while holding a lock on this");
        List<SerializedPageReference> pages;
        synchronized (this) {
            pages = ImmutableList.copyOf(initialPagesForNewBuffers);
            initialPagesForNewBuffers.clear();

            if (outputBuffers.isNoMoreBufferIds()) {
                // verify all created buffers have been declared
                SetView<String> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
                checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
            }
        }

        // dereference outside of synchronized to avoid making a callback while holding a lock
        pages.forEach(SerializedPageReference::dereferencePage);
    }

    private void checkFlushComplete()
    {
        if (state.get() != FLUSHING && state.get() != NO_MORE_BUFFERS) {
            return;
        }

        if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
            destroy();
        }
    }
}
