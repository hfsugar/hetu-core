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

package nova.hetu.shuffle.ucx.message;

import nova.hetu.shuffle.ucx.memory.RegisteredMemory;
import nova.hetu.shuffle.ucx.memory.UcxMemoryPool;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import static nova.hetu.shuffle.ucx.UcxConstant.BYTE_SIZE;
import static nova.hetu.shuffle.ucx.UcxConstant.INT_SIZE;
import static nova.hetu.shuffle.ucx.UcxConstant.LONG_SIZE;
import static nova.hetu.shuffle.ucx.message.UcxMessage.UcxMessageType.PAGE;

public class UcxPageMessage
        extends UcxMessage
{
    // Page metadata is an array of blocks:
    // | blockNumber(4B) | pageCodecMarkers(1B) | offHeap(1B) | positionCount(4B) | uncompressedSizeInBytes(4B) |
    // | block0 | block1 | block2 | block3 | block4 | block5 |
    // Each block in page metadata has next layout:
    // | dataAddress(8B) | dataSize(8B) | dataPositionCount(4B) | dataHashCode(4B) | dataRkeySize(4B) |dataRkey(dataRkeySize Bytes) |

    private static final int PAGE_METADATA_HEADER_SIZE = INT_SIZE * 3 + BYTE_SIZE * 2;
    private static final int BLOCK_METADATA_SIZE = LONG_SIZE * 2 + INT_SIZE + MAX_RKEY_SIZE;
    private final int blockNumber;
    private final byte pageCodecMarkers;
    private final boolean offHeap;
    private final int positionCount;
    private final int uncompressedSizeInBytes;
    private final ArrayList<BlockMetadata> blockMetadataVector = new ArrayList<>();

    public UcxPageMessage(ByteBuffer data)
    {
        super(data);
        this.blockNumber = data.getInt();
        this.pageCodecMarkers = data.get();
        this.offHeap = data.get() == 1;
        this.positionCount = data.getInt();
        this.uncompressedSizeInBytes = data.getInt();
        populateMetadata(data, this.blockNumber);
    }

    @Override
    public int getMaxMessageSize()
    {
        return MAX_MESSAGE_SIZE;
    }

    public BlockMetadata getBlockMetadata(int blockId)
    {
        return blockMetadataVector.get(blockId);
    }

    private void populateMetadata(ByteBuffer metaData, int nbBlocks)
    {
        for (int i = 0; i < nbBlocks; i++) {
            long dataAddress = metaData.getLong();
            long dataSize = metaData.getLong();
            int positionCount = metaData.getInt();
            int hashCode = metaData.getInt();
            int rkeySize = metaData.getInt();
            ByteBuffer dataRkey = metaData.duplicate();
            dataRkey.limit(rkeySize);
            this.blockMetadataVector.add(new BlockMetadata(dataAddress, dataSize, positionCount, dataRkey, hashCode));
        }
    }

    public int getBlockNumber()
    {
        return blockNumber;
    }

    public byte getPageCodecMarkers()
    {
        return pageCodecMarkers;
    }

    public boolean isOffHeap()
    {
        return offHeap;
    }

    public int getPositionCount()
    {
        return positionCount;
    }

    public int getUncompressedSizeInBytes()
    {
        return uncompressedSizeInBytes;
    }

    @Override
    public String toString()
    {
        return "{ blockNumber:" + blockNumber + ",block[0]:" + (blockNumber == 0 ? "null" : getBlockMetadata(0)) +
                ",pageCodecMarkers:" + pageCodecMarkers + ",offHeap:" + offHeap + ",positionCount:" + positionCount + ",uncompressedSizeInBytes:" + uncompressedSizeInBytes + " }";
    }

    public static class BlockMetadata
    {
        private final long dataAddress;
        private final long dataSize;
        private final int positionCount;
        private final ByteBuffer dataRkey;
        private final int hashCode;

        public BlockMetadata(long dataAddress, long dataSize, int positionCount, ByteBuffer dataRkey, int hashCode)
        {
            this.dataAddress = dataAddress;
            this.dataSize = dataSize;
            this.positionCount = positionCount;
            this.dataRkey = dataRkey;
            this.hashCode = hashCode;
        }

        public long getDataAddress()
        {
            return dataAddress;
        }

        public long getDataSize()
        {
            return dataSize;
        }

        public ByteBuffer getDataRkey()
        {
            return dataRkey.duplicate();
        }

        @Override
        public String toString()
        {
            return "{ dataAddress:" +
                    dataAddress +
                    ",dataSize:" +
                    dataSize +
                    ",hashCode:" +
                    hashCode +
                    " }";
        }

        public int getPositionCount()
        {
            return positionCount;
        }
    }

    public static class Builder
            extends UcxMessage.Builder
    {
        private final ArrayList<BlockMetadata> blockMetadataVector = new ArrayList<>();
        private byte pageCodecMarkers;
        private byte offHeap;
        private int positionCount;
        private int uncompressedSizeInBytes;

        public Builder(UcxMemoryPool ucxMemoryPool)
        {
            super(ucxMemoryPool, PAGE);
        }

        public Builder addBlockMetadata(BlockMetadata blockMetadata)
        {
            if (blockMetadata != null) {
                blockMetadataVector.add(blockMetadata);
            }
            return this;
        }

        public Builder setPageCodecMarkers(byte pageCodecMarkers)
        {
            this.pageCodecMarkers = pageCodecMarkers;
            return this;
        }

        public Builder setOffHeap(boolean offHeap)
        {
            this.offHeap = offHeap ? (byte) 1 : (byte) 0;
            return this;
        }

        public Builder setPositionCount(int positionCount)
        {
            this.positionCount = positionCount;
            return this;
        }

        public Builder setUncompressedSizeInBytes(int uncompressedSizeInBytes)
        {
            this.uncompressedSizeInBytes = uncompressedSizeInBytes;
            return this;
        }

        public RegisteredMemory build()
        {
            int bufferSize = PAGE_METADATA_HEADER_SIZE + blockMetadataVector.size() * BLOCK_METADATA_SIZE;
            RegisteredMemory memory = build(bufferSize);
            ByteBuffer buffer = memory.getBuffer();
            buffer.putInt(this.blockMetadataVector.size());
            buffer.put(this.pageCodecMarkers);
            buffer.put(this.offHeap);
            buffer.putInt(this.positionCount);
            buffer.putInt(this.uncompressedSizeInBytes);
            blockMetadataVector.forEach(blockMetadata -> {
                buffer.putLong(blockMetadata.dataAddress);
                buffer.putLong(blockMetadata.dataSize);
                buffer.putInt(blockMetadata.positionCount);
                buffer.putInt(blockMetadata.hashCode);
                buffer.putInt(blockMetadata.dataRkey.capacity());
                buffer.put(blockMetadata.dataRkey);
            });
            buffer.clear();
            return memory;
        }

        @Override
        public String toString()
        {
            return "{ blockNumber:" +
                    blockMetadataVector.size() +
                    ",block[0]:" +
                    (blockMetadataVector.size() == 0 ? "null" : blockMetadataVector.get(0)) +
                    ",pageCodecMarkers:"
                    + pageCodecMarkers
                    + ",offHeap:"
                    + offHeap
                    + ",positionCount:" +
                    +positionCount
                    + ",uncompressedSizeInBytes:"
                    + uncompressedSizeInBytes
                    + " }";
        }
    }
}
