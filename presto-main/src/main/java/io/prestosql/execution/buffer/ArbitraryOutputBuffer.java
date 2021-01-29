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
import io.prestosql.execution.buffer.ClientBuffer.PagesSupplier;
import io.prestosql.memory.context.LocalMemoryContext;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FAILED;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.BufferState.FLUSHING;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_BUFFERS;
import static io.prestosql.execution.buffer.BufferState.NO_MORE_PAGES;
import static io.prestosql.execution.buffer.BufferState.OPEN;
import static io.prestosql.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static io.prestosql.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.util.Objects.requireNonNull;

/**
 * A buffer that assigns pages to queues based on a first come, first served basis.
 */
public class ArbitraryOutputBuffer
        extends OutputBuffer
{
    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);

    private final MasterBuffer masterBuffer;

    @GuardedBy("this")
    private final ConcurrentMap<String, ClientBuffer> buffers = new ConcurrentHashMap<>();
    private final java.lang.String taskInstanceId;

    public ArbitraryOutputBuffer(
            java.lang.String taskInstanceId,
            StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor,
            PagesSerde serde)
    {
        super(state, maxBufferSize, systemMemoryContextSupplier, notificationExecutor, serde);
        this.taskInstanceId = requireNonNull(taskInstanceId, "taskInstanceId is null");
        this.masterBuffer = new MasterBuffer();
    }

    @Override
    public void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener)
    {
        state.addStateChangeListener(stateChangeListener);
    }

    @Override
    public boolean isOverutilized()
    {
        return (memoryManager.getUtilization() >= 0.5) || !state.get().canAddPages();
    }

    @Override
    public OutputBufferStatistics getInfo()
    {
        //
        // NOTE: this code must be lock free so we do not hang for state machine updates
        //

        // always get the state first before any other stats
        BufferState state = this.state.get();

        // buffers it a concurrent collection so it is safe to access out side of guard
        // in this case we only want a snapshot of the current buffers
        @SuppressWarnings("FieldAccessNotGuarded")
        Collection<ClientBuffer> buffers = this.buffers.values();

        int totalBufferedPages = masterBuffer.getBufferedPages();
        ImmutableList.Builder<BufferInfo> infos = ImmutableList.builder();
        for (ClientBuffer buffer : buffers) {
            BufferInfo bufferInfo = buffer.getInfo();
            infos.add(bufferInfo);

            PageBufferInfo pageBufferInfo = bufferInfo.getPageBufferInfo();
            totalBufferedPages += pageBufferInfo.getBufferedPages();
        }

        return new OutputBufferStatistics(
                "ARBITRARY",
                state,
                state.canAddBuffers(),
                state.canAddPages(),
                memoryManager.getBufferedBytes(),
                totalBufferedPages,
                totalRowsAdded.get(),
                totalPagesAdded.get(),
                infos.build());
    }

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
            for (String outputBufferId : outputBuffers.getBuffers().keySet()) {
                getBuffer(outputBufferId);
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

    void doEnqueue(int partition, List<SerializedPage> pages)
    {
        checkState(!Thread.holdsLock(this), "Can not enqueue pages while holding a lock on this");
        requireNonNull(pages, "page is null");

        // create page reference counts with an initial single reference
        List<SerializedPageReference> serializedPageReferences = pages.stream()
                .map(pageSplit -> new SerializedPageReference(pageSplit, 1, () -> memoryManager.updateMemoryUsage(-pageSplit.getRetainedSizeInBytes())))
                .collect(toImmutableList());

        // add pages to the buffer (this will increase the reference count by one)
        masterBuffer.addPages(serializedPageReferences);

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            if (masterBuffer.isEmpty()) {
                break;
            }
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }
    }

    @Override
    public ListenableFuture<BufferResult> get(String bufferId, long startingSequenceId, DataSize maxSize)
    {
        checkState(!Thread.holdsLock(this), "Can not get pages while holding a lock on this");
        requireNonNull(bufferId, "bufferId is null");
        checkArgument(maxSize.toBytes() > 0, "maxSize must be at least 1 byte");

        return getBuffer(bufferId).getPages(startingSequenceId, maxSize, Optional.of(masterBuffer));
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

        masterBuffer.setNoMorePages();

        // process any pending reads from the client buffers
        for (ClientBuffer clientBuffer : safeGetBuffersSnapshot()) {
            clientBuffer.loadPagesIfNecessary(masterBuffer);
        }

        checkFlushComplete();
    }

    @Override
    public void destroy()
    {
        checkState(!Thread.holdsLock(this), "Can not destroy while holding a lock on this");

        // ignore destroy if the buffer already in a terminal state.
        if (state.setIf(FINISHED, oldState -> !oldState.isTerminal())) {
            noMoreBuffers();

            masterBuffer.destroy();

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
        checkState(state.get().canAddBuffers() || !outputBuffers.isNoMoreBufferIds(), "No more buffers already set");

        // NOTE: buffers are allowed to be created before they are explicitly declared by setOutputBuffers
        // When no-more-buffers is set, we verify that all created buffers have been declared
        buffer = new ClientBuffer(taskInstanceId, id);

        // buffer may have finished immediately before calling this method
        if (state.get() == FINISHED) {
            buffer.destroy();
        }

        buffers.put(id, buffer);
        return buffer;
    }

    private synchronized Collection<ClientBuffer> safeGetBuffersSnapshot()
    {
        return ImmutableList.copyOf(this.buffers.values());
    }

    private synchronized void noMoreBuffers()
    {
        if (outputBuffers.isNoMoreBufferIds()) {
            // verify all created buffers have been declared
            SetView<String> undeclaredCreatedBuffers = Sets.difference(buffers.keySet(), outputBuffers.getBuffers().keySet());
            checkState(undeclaredCreatedBuffers.isEmpty(), "Final output buffers does not contain all created buffer ids: %s", undeclaredCreatedBuffers);
        }
    }

    @GuardedBy("this")
    private void checkFlushComplete()
    {
        // This buffer type assigns each page to a single, arbitrary reader,
        // so we don't need to wait for no-more-buffers to finish the buffer.
        // Any readers added after finish will simply receive no data.
        BufferState state = this.state.get();
        if ((state == FLUSHING) || ((state == NO_MORE_PAGES) && masterBuffer.isEmpty())) {
            if (safeGetBuffersSnapshot().stream().allMatch(ClientBuffer::isDestroyed)) {
                destroy();
            }
        }
    }

    @ThreadSafe
    private static class MasterBuffer
            implements PagesSupplier
    {
        @GuardedBy("this")
        private final LinkedList<SerializedPageReference> masterBuffer = new LinkedList<>();

        @GuardedBy("this")
        private boolean noMorePages;

        private final AtomicInteger bufferedPages = new AtomicInteger();

        public synchronized void addPages(List<SerializedPageReference> pages)
        {
            masterBuffer.addAll(pages);
            bufferedPages.set(masterBuffer.size());
        }

        public synchronized boolean isEmpty()
        {
            return masterBuffer.isEmpty();
        }

        @Override
        public synchronized boolean mayHaveMorePages()
        {
            return !noMorePages || !masterBuffer.isEmpty();
        }

        public synchronized void setNoMorePages()
        {
            this.noMorePages = true;
        }

        @Override
        public synchronized List<SerializedPageReference> getPages(DataSize maxSize)
        {
            long maxBytes = maxSize.toBytes();
            List<SerializedPageReference> pages = new ArrayList<>();
            long bytesRemoved = 0;

            while (true) {
                SerializedPageReference page = masterBuffer.peek();
                if (page == null) {
                    break;
                }
                bytesRemoved += page.getRetainedSizeInBytes();
                // break (and don't add) if this page would exceed the limit
                if (!pages.isEmpty() && bytesRemoved > maxBytes) {
                    break;
                }
                // this should not happen since we have a lock
                checkState(masterBuffer.poll() == page, "Master buffer corrupted");
                pages.add(page);
            }

            bufferedPages.set(masterBuffer.size());

            return ImmutableList.copyOf(pages);
        }

        public void destroy()
        {
            checkState(!Thread.holdsLock(this), "Can not destroy master buffer while holding a lock on this");
            List<SerializedPageReference> pages;
            synchronized (this) {
                pages = ImmutableList.copyOf(masterBuffer);
                masterBuffer.clear();
                bufferedPages.set(0);
            }

            // dereference outside of synchronized to avoid making a callback while holding a lock
            pages.forEach(SerializedPageReference::dereferencePage);
        }

        public int getBufferedPages()
        {
            return bufferedPages.get();
        }

        @Override
        public java.lang.String toString()
        {
            return toStringHelper(this)
                    .add("bufferedPages", bufferedPages.get())
                    .toString();
        }
    }
}
