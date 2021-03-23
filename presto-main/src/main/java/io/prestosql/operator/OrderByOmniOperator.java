package io.prestosql.operator;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.*;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.PlanNodeId;
import nova.hetu.omnicache.runtime.OMResult;
import nova.hetu.omnicache.runtime.OmniOrderBy;
import nova.hetu.omnicache.vector.DoubleVec;
import nova.hetu.omnicache.vector.IntVec;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterators.transform;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class OrderByOmniOperator
        implements Operator
{
    public static class OrderByOmniOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final List<Type> sourceTypes;
        private final List<Integer> outputChannels;
        private final List<Integer> sortChannels;
        private final List<SortOrder> sortOrder;
        private final OmniOrderBy omniOrderBy;
        private boolean closed;

        public OrderByOmniOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                List<? extends Type> sourceTypes,
                List<Integer> outputChannels,
                List<Integer> sortChannels,
                List<SortOrder> sortOrder)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.sourceTypes = ImmutableList.copyOf(requireNonNull(sourceTypes, "sourceTypes is null"));
            this.outputChannels = requireNonNull(outputChannels, "outputChannels is null");
            this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
            this.sortOrder = ImmutableList.copyOf(requireNonNull(sortOrder, "sortOrder is null"));
            this.omniOrderBy = new OmniOrderBy();
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, OrderByOmniOperator.class.getSimpleName());
            return new OrderByOmniOperator(
                    operatorContext,
                    sourceTypes,
                    outputChannels,
                    sortChannels,
                    sortOrder,
                    omniOrderBy);
        }

        @Override
        public void noMoreOperators() {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate() {
            return new OrderByOmniOperatorFactory(
                    operatorId,
                    planNodeId,
                    sourceTypes,
                    outputChannels,
                    sortChannels,
                    sortOrder);
        }
    }

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }

    private final OperatorContext operatorContext;
    private final int[] outputChannels;
    private final LocalMemoryContext revocableMemoryContext;
    private final LocalMemoryContext localUserMemoryContext;
    private final List<Type> sourceTypes;
    private final OmniOrderBy omniOrderBy;
    private final long sortAddress;
    private Iterator<Optional<Page>> sortedPages = null;
    private State state = State.NEEDS_INPUT;

    public OrderByOmniOperator(OperatorContext operatorContext,
                               List<Type> sourceTypes,
                               List<Integer> outputChannels,
                               List<Integer> sortChannels,
                               List<SortOrder> sortOrder,
                               OmniOrderBy omniOrderBy)
    {
        this.operatorContext = operatorContext;
        this.sourceTypes = sourceTypes;
        this.outputChannels = Ints.toArray(requireNonNull(outputChannels, "outputChannels is null"));
        this.omniOrderBy = omniOrderBy;
        this.localUserMemoryContext = operatorContext.localUserMemoryContext();
        this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();

        int sortColSize = sortChannels.size();
        int[] ascendings = new int[sortColSize];
        int[] nullFirsts = new int[sortColSize];
        for (int i = 0; i < sortColSize; i++) {
            SortOrder order = sortOrder.get(i);
            ascendings[i] = order.isAscending() ? 1 : 0;
            nullFirsts[i] = order.isNullsFirst() ? 1 : 0;
        }

        int[] types = new int[sourceTypes.size()];
        for (int i = 0; i < sourceTypes.size(); i++) {
            types[i] = getTypeIdx(sourceTypes.get(i));
        }

        this.sortAddress = omniOrderBy.allocAndInitSort(types,
                sourceTypes.size(),
                this.outputChannels,
                this.outputChannels.length,
                Ints.toArray(sortChannels),
                ascendings,
                nullFirsts,
                sortColSize);
    }

    private int getTypeIdx(Type type)
    {
        String base = type.getTypeSignature().getBase();
        if (base.equals(StandardTypes.INTEGER)) {
            return 1;
        }
        else if (base.equals(StandardTypes.BIGINT)) {
            return 2;
        }
        else if (base.equals(StandardTypes.DOUBLE)) {
            return 3;
        }
        else {
            return 1;
        }
    }

    @Override
    public OperatorContext getOperatorContext() {
        return operatorContext;
    }

    @Override
    public boolean needsInput() {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page) {
        checkState(state == State.NEEDS_INPUT, "Operator is already finishing");
        requireNonNull(page, "page is null");

//        long start = System.currentTimeMillis();
        int channelCount = page.getChannelCount();
        int positionCount = page.getPositionCount();
        Vec[] dataVecs = new Vec[channelCount];
        Vec[] nullVecs = new Vec[channelCount];  //null vec for all columns

        for (int i = 0; i < channelCount; i++) {
            Vec vec = page.getBlock(i).getValues();
            dataVecs[i] = vec;
            IntVec nullVec = new IntVec(positionCount);
            nullVecs[i] = nullVec;
        }

//        System.out.println("OrderByOmniOperator before add Table elapsed time : " + (System.currentTimeMillis() - start) + " ms");
        // transform page to void **data
        omniOrderBy.addTable(sortAddress, dataVecs, nullVecs);
//        System.out.println("OrderByOmniOperator after add table elapsed time : " + (System.currentTimeMillis() - start) + " ms");
    }

    @Override
    public Page getOutput()
    {
        if (state != State.HAS_OUTPUT) {
            return null;
        }

        if (sortedPages == null) {
//            long start = System.currentTimeMillis();
            OMResult result = omniOrderBy.getResult(sortAddress);
//            System.out.println("OrderByOmniOperator Get result elapsed time " + (System.currentTimeMillis() - start) + " ms");
            Block[] blocks = getBlocks(result);

            List<Type> outputTypes = new ArrayList<>();
            for (int i = 0; i < outputChannels.length; i++) {
                outputTypes.add(sourceTypes.get(outputChannels[i]));
            }

            Iterator<Page> sortedPagesIndex = getSortedPages(outputTypes, blocks, blocks[0].getPositionCount());
            sortedPages = transform(sortedPagesIndex, Optional::of);
//            System.out.println("OrderByOmniOperator Get sorted pages elapsed time " + (System.currentTimeMillis() - start) + " ms");
        }

//        long start1 = System.currentTimeMillis();
        if (!sortedPages.hasNext()) {
            state = State.FINISHED;
            return null;
        }

        Optional<Page> next = sortedPages.next();
        if (!next.isPresent()) {
            return null;
        }
        Page nextPage = next.get();
        Block[] blocks = new Block[outputChannels.length];
        for (int i = 0; i < outputChannels.length; i++) {
            blocks[i] = nextPage.getBlock(i);
        }
//        System.out.println("OrderByOmniOperator Get output elapsed time " + (System.currentTimeMillis() - start1) + " ms");
        return new Page(nextPage.getPositionCount(), blocks);
    }

    @Override
    public void finish() {
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;

            // Convert revocable memory to user memory as sortedPages holds on to memory so we no longer can revoke.
            if (revocableMemoryContext.getBytes() > 0) {
                long currentRevocableBytes = revocableMemoryContext.getBytes();
                revocableMemoryContext.setBytes(0);
                if (!localUserMemoryContext.trySetBytes(localUserMemoryContext.getBytes() + currentRevocableBytes)) {
                    // TODO: this might fail (even though we have just released memory), but we don't
                    // have a proper way to atomically convert memory reservations
                    revocableMemoryContext.setBytes(currentRevocableBytes);
                    // spill since revocable memory could not be converted to user memory immediately
                    // TODO: this should be asynchronous
                }
            }
//            long start = System.currentTimeMillis();
            omniOrderBy.sort(sortAddress);
//            long elapsed = System.currentTimeMillis() - start;
//            System.out.println("OrderByOmniOperator OrderByOmniOperator finish() sort spend : " + elapsed + "ms");
        }
    }

    @Override
    public boolean isFinished() {
        return state == State.FINISHED;
    }

    private Block[] getBlocks(OMResult result)
    {
        int colNum = outputChannels.length;
        int rowNum = result.getLength();

        boolean[] valueIsNull = new boolean[rowNum];
        for (int i = 0; i < rowNum; i++) {
            valueIsNull[i] = false;
        }

        Block[] blocks = new Block[colNum];
        for (int idx = 0; idx < colNum; idx++) {
            ByteBuffer buffer = result.getBuffers()[idx];
            buffer.order(ByteOrder.LITTLE_ENDIAN);

            Type type = sourceTypes.get(outputChannels[idx]);
            String base = type.getTypeSignature().getBase();
            switch (base) {
                case StandardTypes.INTEGER:
                    IntVec intVec = new IntVec(buffer, rowNum);
                    blocks[idx] = new IntArrayBlock(rowNum, Optional.of(valueIsNull), intVec);
                    break;
                case StandardTypes.BIGINT:
                    LongVec longVec = new LongVec(buffer, rowNum);
                    blocks[idx] = new LongArrayBlock(rowNum, Optional.of(valueIsNull), longVec);
                    break;
                case StandardTypes.DOUBLE:
                    DoubleVec doubleVec = new DoubleVec(buffer, rowNum);
                    blocks[idx] = new DoubleArrayBlock(rowNum, Optional.of(valueIsNull), doubleVec);
                    break;
                default:
                    throw new IllegalArgumentException(format("Not Support Vec Type %s", base));
            }
        }

        return blocks;
    }

    private Iterator<Page> getSortedPages(List<Type> outputTypes, Block[] blocks, int positionCount)
    {
        return new AbstractIterator<Page>()
        {
            private int currentPosition;
            private final PageBuilder pageBuilder = new PageBuilder(outputTypes);

            @Override
            public Page computeNext()
            {
                currentPosition = buildPage(currentPosition, pageBuilder, blocks, positionCount);
                if (pageBuilder.isEmpty()) {
                    return endOfData();
                }
                Page page = pageBuilder.build();
                pageBuilder.reset();
                return page;
            }
        };
    }

    private int buildPage(int position, PageBuilder pageBuilder, Block[] blocks, int positionCount)
    {
        while (!pageBuilder.isFull() && position < positionCount) {
            // append the row
            pageBuilder.declarePosition();
            for (int i = 0; i < outputChannels.length; i++) {
                int outputChannel = outputChannels[i];
                Type type = sourceTypes.get(outputChannel);
                Block block = blocks[i];
                type.appendTo(block, position, pageBuilder.getBlockBuilder(i));
            }

            position++;
        }

        return position;
    }
}
