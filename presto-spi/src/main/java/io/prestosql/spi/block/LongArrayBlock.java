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
package io.prestosql.spi.block;

import io.prestosql.spi.util.BloomFilter;
import nova.hetu.omnicache.vector.LongVec;
import nova.hetu.omnicache.vector.Vec;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.prestosql.spi.block.BlockUtil.checkArrayRange;
import static io.prestosql.spi.block.BlockUtil.checkValidRegion;
import static io.prestosql.spi.block.BlockUtil.compactArray;
import static io.prestosql.spi.block.BlockUtil.countUsedPositions;
import static java.lang.Math.toIntExact;

public class LongArrayBlock
        implements Block<Long>
{
    //can we add block index to speed up operations?
    //statistics?
    //can we intro operations at block level?, for example join of blocks?
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongArrayBlock.class).instanceSize();

    private final int arrayOffset;
    private final int positionCount;
    @Nullable
    private final boolean[] valueIsNull;
    private final LongVec values;

    private final long sizeInBytes;
    private final long retainedSizeInBytes;

    public LongArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, long[] values)
    {
        this(0, positionCount, valueIsNull.orElse(null), values);
    }

    public LongArrayBlock(int positionCount, Optional<boolean[]> valueIsNull, LongVec longVec)
    {
        this(0, positionCount, valueIsNull.orElse(null), longVec);
    }

    public LongArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, long[] values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        //TODO:Currently OmniCodegen Vec is not support offset,and reuse same vec,future try to avoid copy here
        this.values = new LongVec(positionCount);
        this.values.getData().asLongBuffer().put(values, arrayOffset, positionCount);

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        if (arrayOffset > 0 && valueIsNull != null) {
            this.valueIsNull = new boolean[positionCount];
            System.arraycopy(valueIsNull, arrayOffset, this.valueIsNull, 0, positionCount);
        }
        else {
            this.valueIsNull = valueIsNull;
        }

        this.arrayOffset = 0;

        sizeInBytes = (Long.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + sizeOf(values);
    }

    public LongArrayBlock(int arrayOffset, int positionCount, boolean[] valueIsNull, LongVec values)
    {
        if (arrayOffset < 0) {
            throw new IllegalArgumentException("arrayOffset is negative");
        }
        if (positionCount < 0) {
            throw new IllegalArgumentException("positionCount is negative");
        }
        this.positionCount = positionCount;

        if (values.size() - arrayOffset < positionCount) {
            throw new IllegalArgumentException("values length is less than positionCount");
        }

        if (arrayOffset > 0) {
            //there is no need to close the original Vec values,since it will be closed by its owner(eg. the left part of this Vec)
            //although its owner may not own the whole part of the Vec
            this.values = values.slice(arrayOffset, arrayOffset + positionCount);
        }
        else {
            this.values = values;
        }

        if (valueIsNull != null && valueIsNull.length - arrayOffset < positionCount) {
            throw new IllegalArgumentException("isNull length is less than positionCount");
        }
        if (arrayOffset > 0 && valueIsNull != null) {
            this.valueIsNull = new boolean[positionCount];
            System.arraycopy(valueIsNull, arrayOffset, this.valueIsNull, 0, positionCount);
        }
        else {
            this.valueIsNull = valueIsNull;
        }

        this.arrayOffset = 0;

        sizeInBytes = (Long.BYTES + Byte.BYTES) * (long) positionCount;
        retainedSizeInBytes = INSTANCE_SIZE + sizeOf(valueIsNull) + this.values.capacity();
    }

    @Override
    public LongVec getValues()
    {
        return values;
    }

    @Override
    public long getSizeInBytes()
    {
        return sizeInBytes;
    }

    @Override
    public long getRegionSizeInBytes(int position, int length)
    {
        return (Long.BYTES + Byte.BYTES) * (long) length;
    }

    @Override
    public long getPositionsSizeInBytes(boolean[] positions)
    {
        return (Long.BYTES + Byte.BYTES) * (long) countUsedPositions(positions);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return retainedSizeInBytes;
    }

    @Override
    public long getEstimatedDataSizeForStats(int position)
    {
        return isNull(position) ? 0 : Long.BYTES;
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        // TODO: try to avoid copy here
        long[] valuesArray = new long[values.size()];
        values.getData().asLongBuffer().get(valuesArray);
        consumer.accept(valuesArray, sizeOf(valuesArray));
        if (valueIsNull != null) {
            consumer.accept(valueIsNull, sizeOf(valueIsNull));
        }
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getLong(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return values.get(position + arrayOffset);
    }

    @Override
    public Vec getVec()
    {
        return this.values;
    }

    @Override
    public boolean isOffHeap()
    {
        return true;
    }

    public Long get(int position)
    {
        if (valueIsNull != null && valueIsNull[position + arrayOffset]) {
            return null;
        }
        return values.get(position + arrayOffset);
    }

    @Override
    @Deprecated
    // TODO: Remove when we fix intermediate types on aggregations.
    public int getInt(int position, int offset)
    {
        checkReadablePosition(position);
        if (offset != 0) {
            throw new IllegalArgumentException("offset must be zero");
        }
        return toIntExact(values.get(position + arrayOffset));
    }

    @Override
    public boolean mayHaveNull()
    {
        return valueIsNull != null;
    }

    @Override
    public boolean isNull(int position)
    {
        checkReadablePosition(position);
        return valueIsNull != null && valueIsNull[position + arrayOffset];
    }

    @Override
    public void writePositionTo(int position, BlockBuilder blockBuilder)
    {
        checkReadablePosition(position);
        blockBuilder.writeLong(values.get(position + arrayOffset));
        blockBuilder.closeEntry();
    }

    @Override
    public Block getSingleValueBlock(int position)
    {
        checkReadablePosition(position);
        return new LongArrayBlock(
                0,
                1,
                isNull(position) ? new boolean[] {true} : null,
                new long[] {values.get(position + arrayOffset)});
    }

    @Override
    public Block copyPositions(int[] positions, int offset, int length)
    {
        checkArrayRange(positions, offset, length);

        boolean[] newValueIsNull = null;
        if (valueIsNull != null) {
            newValueIsNull = new boolean[length];
        }
        long[] newValues = new long[length];
        for (int i = 0; i < length; i++) {
            int position = positions[offset + i];
            checkReadablePosition(position);
            if (valueIsNull != null) {
                newValueIsNull[i] = valueIsNull[position + arrayOffset];
            }
            newValues[i] = values.get(position + arrayOffset);
        }
        return new LongArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public Block getRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        return new LongArrayBlock(positionOffset + arrayOffset, length, valueIsNull, values);
    }

    @Override
    public Block copyRegion(int positionOffset, int length)
    {
        checkValidRegion(getPositionCount(), positionOffset, length);
        positionOffset += arrayOffset;

        LongVec newValues = new LongVec(length);
        for (int i = 0; i < length; i++) {
            newValues.set(i, this.values.get(positionOffset + i));
        }
        boolean[] newValueIsNull = valueIsNull == null ? null : compactArray(valueIsNull, positionOffset, length);

        if (newValueIsNull == valueIsNull && newValues == values) {
            return this;
        }
        return new LongArrayBlock(0, length, newValueIsNull, newValues);
    }

    @Override
    public String getEncodingName()
    {
        return LongArrayBlockEncoding.NAME;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("LongArrayBlock{");
        sb.append("positionCount=").append(getPositionCount());
        sb.append('}');
        return sb.toString();
    }

    private void checkReadablePosition(int position)
    {
        if (position < 0 || position >= getPositionCount()) {
            throw new IllegalArgumentException("position is not valid");
        }
    }

    @Override
    public boolean[] filter(BloomFilter filter, boolean[] validPositions)
    {
        for (int i = 0; i < values.size(); i++) {
            validPositions[i] = validPositions[i] && filter.test(values.get(i));
        }

        return validPositions;
    }

    @Override
    public int filter(int[] positions, int positionCount, int[] matchedPositions, Function<Object, Boolean> test)
    {
        int matchCount = 0;
        for (int i = 0; i < positionCount; i++) {
            if (valueIsNull != null && valueIsNull[positions[i] + arrayOffset]) {
                if (test.apply(null)) {
                    matchedPositions[matchCount++] = positions[i];
                }
            }
            else if (test.apply(values.get(positions[i] + arrayOffset))) {
                matchedPositions[matchCount++] = positions[i];
            }
        }

        return matchCount;
    }
}
