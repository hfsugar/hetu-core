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
package io.hetu.core.transport.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import org.openjdk.jol.info.ClassLayout;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.hetu.core.transport.execution.buffer.PageCodecMarker.COMPRESSED;
import static io.hetu.core.transport.execution.buffer.PageCodecMarker.ENCRYPTED;
import static io.hetu.core.transport.execution.buffer.PageCodecMarker.MarkerSet.fromByteValue;
import static java.util.Objects.requireNonNull;

public class SerializedPage
        implements Closeable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SerializedPage.class).instanceSize();
    private final Page page;
    private Slice slice;
    private Block[] blocks;
    private int positionCount;
    private int uncompressedSizeInBytes;
    private byte pageCodecMarkers;
    private Properties pageMetadata = new Properties();

    @JsonCreator
    public SerializedPage(
            @JsonProperty("sliceArray") byte[] sliceArray,
            @JsonProperty("pageCodecMarkers") byte pageCodecMarkers,
            @JsonProperty("positionCount") int positionCount,
            @JsonProperty("uncompressedSizeInBytes") int uncompressedSizeInBytes,
            @JsonProperty("pageMetadata") Properties pageMetadata)
    {
        this(
                Slices.wrappedBuffer(sliceArray),
                fromByteValue(pageCodecMarkers),
                positionCount,
                uncompressedSizeInBytes,
                pageMetadata,
                null);
    }

    public SerializedPage(byte[] sliceArray, byte pageCodecMarkers, int positionCount, int uncompressedSizeInBytes)
    {
        this(
                Slices.wrappedBuffer(sliceArray),
                fromByteValue(pageCodecMarkers),
                positionCount,
                uncompressedSizeInBytes);
    }

    public SerializedPage(byte[] sliceArray, byte pageCodecMarkers, int positionCount, int uncompressedSizeInBytes, Page page)
    {
        this(
                Slices.wrappedBuffer(sliceArray),
                fromByteValue(pageCodecMarkers),
                positionCount,
                uncompressedSizeInBytes,
                page);
    }

    public SerializedPage(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes, Properties pageMetadata, Page page)
    {
        this.page = page;
        populate(slice, markers, positionCount, uncompressedSizeInBytes, pageMetadata);
    }

    public SerializedPage(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes)
    {
        this(slice, markers, positionCount, uncompressedSizeInBytes, null, null);
    }

    public SerializedPage(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes, Page page)
    {
        this(slice, markers, positionCount, uncompressedSizeInBytes, null, page);
    }

    public SerializedPage(Block[] blocks, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes, Properties pageMetadata, Page page)
    {
        this.blocks = requireNonNull(blocks, "blocks is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.pageCodecMarkers = requireNonNull(markers, "markers is null").byteValue();
        this.pageMetadata = pageMetadata == null ? new Properties() : pageMetadata;
        this.page = page;
    }

    public void populate(Slice slice, PageCodecMarker.MarkerSet markers, int positionCount, int uncompressedSizeInBytes, Properties pageMetadata)
    {
        this.slice = requireNonNull(slice, "slice is null");
        this.positionCount = positionCount;
        checkArgument(uncompressedSizeInBytes >= 0, "uncompressedSizeInBytes is negative");
        this.uncompressedSizeInBytes = uncompressedSizeInBytes;
        this.pageCodecMarkers = requireNonNull(markers, "markers is null").byteValue();
        this.pageMetadata = pageMetadata == null ? new Properties() : pageMetadata;
        //  Encrypted pages may include arbitrary overhead from ciphers, sanity checks skipped
        if (!markers.contains(ENCRYPTED)) {
            if (markers.contains(COMPRESSED)) {
                checkArgument(uncompressedSizeInBytes > slice.length(), "compressed size must be smaller than uncompressed size when compressed");
            }
            else {
                checkArgument(uncompressedSizeInBytes == slice.length(), "uncompressed size must be equal to slice length when uncompressed");
            }
        }
    }

    public void acquire()
    {
        if (page != null) {
            // we acquire the page to make sure the block don't get released, this will be released as part of UCX resource free ops
            page.acquire();
        }
    }

    public int getSizeInBytes()
    {
        if (slice != null) {
            return slice.length();
        }
        else {
            int size = 0;
            for (Block block : blocks) {
                size += block.getSizeInBytes();
            }
            return size;
        }
    }

    public Page getRawPageReference()
    {
        return page;
    }

    @JsonProperty
    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    public long getRetainedSizeInBytes()
    {
        if (slice != null) {
            return INSTANCE_SIZE + slice.getRetainedSize();
        }
        else {
            int size = 0;
            for (Block block : blocks) {
                size += block.getRetainedSizeInBytes();
            }
            return size;
        }
    }

    @JsonProperty
    public int getPositionCount()
    {
        return positionCount;
    }

    @JsonProperty
    public byte[] getSliceArray()
    {
        if (slice != null) {
            return slice.getBytes();
        }
        else {
            return null;
        }
    }

    public Slice getSlice()
    {
        return slice;
    }

    @JsonProperty
    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    public boolean isCompressed()
    {
        return COMPRESSED.isSet(pageCodecMarkers);
    }

    public boolean isEncrypted()
    {
        return ENCRYPTED.isSet(pageCodecMarkers);
    }

    public boolean isOffHeap()
    {
        return slice == null && blocks != null && blocks.length != 0;
    }

    public Block[] getBlocks()
    {
        return blocks;
    }

    @JsonProperty
    public Properties getPageMetadata()
    {
        return pageMetadata;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("positionCount", positionCount)
                .add("pageCodecMarkers", PageCodecMarker.toSummaryString(pageCodecMarkers))
                .add("sizeInBytes", slice.length())
                .add("uncompressedSizeInBytes", uncompressedSizeInBytes)
                .add("pageMetadata", pageMetadata)
                .toString();
    }

    @Override
    public void close()
            throws IOException
    {
        if (page != null) {
            page.release();
        }
    }
}
