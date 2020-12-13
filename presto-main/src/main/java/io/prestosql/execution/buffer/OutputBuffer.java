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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.hetu.core.transport.execution.buffer.PagesSerde;
import io.hetu.core.transport.execution.buffer.SerializedPage;
import io.prestosql.execution.StateMachine;
import io.prestosql.execution.StateMachine.StateChangeListener;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.execution.buffer.BufferState.FINISHED;
import static io.prestosql.execution.buffer.PageSplitterUtil.splitPage;
import static io.prestosql.spi.block.PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES;
import static java.util.Objects.requireNonNull;

public abstract class OutputBuffer
{
    protected final PagesSerde serde;
    protected final StateMachine<BufferState> state;
    protected final OutputBufferMemoryManager memoryManager;
    protected final Supplier<LocalMemoryContext> systemMemoryContextSupplier;
    protected final AtomicLong totalPagesAdded = new AtomicLong();
    protected final AtomicLong totalRowsAdded = new AtomicLong();
    protected final AtomicLong totalBufferedPages = new AtomicLong();
    protected final DataSize maxBufferSize;
    protected final Executor executor;

    public OutputBuffer(StateMachine<BufferState> state,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor,
            PagesSerde serde)
    {
        this.state = state;
        this.serde = serde;
        this.maxBufferSize = maxBufferSize;
        this.systemMemoryContextSupplier = systemMemoryContextSupplier;
        this.executor = notificationExecutor;

        this.memoryManager = new OutputBufferMemoryManager(
                requireNonNull(maxBufferSize, "maxBufferSize is null").toBytes(),
                requireNonNull(systemMemoryContextSupplier, "systemMemoryContextSupplier is null"),
                requireNonNull(notificationExecutor, "notificationExecutor is null"));
    }

    protected void recordStatistics(List<SerializedPage> pages)
    {
        // reserve memory
        long bytesAdded = pages.stream().mapToLong(SerializedPage::getRetainedSizeInBytes).sum();
        memoryManager.updateMemoryUsage(bytesAdded);

        // update stats
        long rowCount = pages.stream().mapToLong(SerializedPage::getPositionCount).sum();
        totalRowsAdded.addAndGet(rowCount);
        totalPagesAdded.addAndGet(pages.size());
        totalBufferedPages.addAndGet(pages.size());
    }

    /**
     * Gets the current state of this buffer.  This method is guaranteed to not block or acquire
     * contended locks, but the stats in the info object may be internally inconsistent.
     */
    public abstract OutputBufferStatistics getInfo();

    /**
     * A buffer is finished once no-more-pages has been set and all buffers have been closed
     * with an abort call.
     */
    public boolean isFinished()
    {
        return state.get() == FINISHED;
    }

    /**
     * Get the memory utilization percentage.
     */
    public double getUtilization()
    {
        return memoryManager.getUtilization();
    }

    /**
     * Check if the buffer is blocking producers.
     */
    public boolean isOverutilized()
    {
        return memoryManager.isOverutilized();
    }

    /**
     * Add a listener which fires anytime the buffer state changes.
     * Listener is always notified asynchronously using a dedicated notification thread pool so, care should
     * be taken to avoid leaking {@code this} when adding a listener in a constructor. Additionally, it is
     * possible notifications are observed out of order due to the asynchronous execution.
     */
    public abstract void addStateChangeListener(StateChangeListener<BufferState> stateChangeListener);

    /**
     * Updates the buffer configuration.
     */
    public abstract void setOutputBuffers(OutputBuffers newOutputBuffers, PagesSerde serde);

    /**
     * Gets pages from the output buffer, and acknowledges all pages received from the last
     * request.  The initial token is zero. Subsequent tokens are acquired from the
     * next token field in the BufferResult returned from the previous request.
     * If the buffer result is marked as complete, the client must call abort to acknowledge
     * receipt of the final state.
     */
    public abstract ListenableFuture<BufferResult> get(String bufferId, long token, DataSize maxSize);

    /**
     * Acknowledges the previously received pages from the output buffer.
     */
    public abstract void acknowledge(String bufferId, long token);

    /**
     * Closes the specified output buffer.
     */
    public abstract void abort(String bufferId);

    /**
     * Get a future that will be completed when the buffer is not full.
     */
    public ListenableFuture<?> isFull()
    {
        return memoryManager.getBufferBlockedFuture();
    }

    /**
     * Adds a split-up page to a specific partition.  If no-more-pages has been set, the enqueue
     * page call is ignored.  This can happen with limit queries.
     */
    abstract void doEnqueue(int partition, List<SerializedPage> pages);

    /**
     * all metric collection is done here to decouple from the real enqueue logic, the real work is done in {@link #doEnqueue(int, List)}
     *
     * @param page
     */
    public void enqueue(int partition, Page page)
    {
        checkState(!Thread.holdsLock(this), "Can not enqueue pages while holding a lock on this");
        requireNonNull(page, "pages is null");

        // ignore pages after "no more pages" is set
        // this can happen with a limit query
        if (!state.get().canAddPages()) {
            return;
        }

        List<SerializedPage> pages = serialize(page);
        recordStatistics(pages);
        doEnqueue(partition, pages);
    }

    public void enqueue(Page page)
    {
        enqueue(0, page);
    }

    protected List<SerializedPage> serialize(Page page)
    {
        return splitPage(page, DEFAULT_MAX_PAGE_SIZE_IN_BYTES).stream()
                .map(serde::serialize)
                .collect(toImmutableList());
    }

    /**
     * Notify buffer that no more pages will be added. Any future calls to enqueue a
     * page are ignored.
     */
    public abstract void setNoMorePages();

    /**
     * Destroys the buffer, discarding all pages.
     */
    public abstract void destroy();

    /**
     * Fail the buffer, discarding all pages, but blocking readers.  It is expected that
     * readers will be unblocked when the failed query is cleaned up.
     */
    public abstract void fail();

    /**
     * @return the peak memory usage of this output buffer.
     */
    public long getPeakMemoryUsage()
    {
        return memoryManager.getPeakMemoryUsage();
    }

    @VisibleForTesting
    void forceFreeMemory()
    {
        memoryManager.close();
    }

    @VisibleForTesting
    OutputBufferMemoryManager getMemoryManager()
    {
        return memoryManager;
    }
}
