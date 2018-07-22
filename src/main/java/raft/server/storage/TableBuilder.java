package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class TableBuilder {
    private FileChannel fileChannel;
    private BlockBuilder dataBlock;
    private BlockBuilder indexBlock;
    private BlockHandle pendingIndexBlockHandle;
    private int lastKey = -1;
    private long offset;
    private boolean isFinished;

    TableBuilder(FileChannel fileChannel) {
        assert fileChannel != null;
        this.fileChannel = fileChannel;
        dataBlock = new BlockBuilder();
        indexBlock = new BlockBuilder();
    }

    void add(int k, byte[] v) throws IOException {
        assert k > lastKey;
        assert v.length > 0;
        assert !isFinished;

        if (pendingIndexBlockHandle != null) {
            indexBlock.add(k, pendingIndexBlockHandle.encode());
            pendingIndexBlockHandle = null;
        }

        dataBlock.add(k, v);

        if (dataBlock.getCurrentEstimateBlockSize() >= Constant.kMaxBlockSize) {
            flushDataBlock();
        }

        lastKey = k;
    }

    private void flushDataBlock() throws IOException {
        assert ! dataBlock.isEmpty();
        pendingIndexBlockHandle = writeBlock(dataBlock);
        fileChannel.force(true);
    }

    private BlockHandle writeBlock(BlockBuilder block) throws IOException{
        BlockHandle handle = new BlockHandle();
        handle.setOffset(offset);

        long checksum = block.writeBlock(fileChannel);
        int blockSize = block.getBlockSize();
        handle.setSize(blockSize);

        // write tailer
        ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        trailer.putLong(checksum);
        trailer.flip();
        fileChannel.write(trailer);

        dataBlock.reset();
        offset += blockSize + Constant.kBlockTrailerSize;

        return handle;
    }

    long finishBuild() throws IOException {
        assert ! isFinished;
        assert offset > 0;

        isFinished = true;

        if (!dataBlock.isEmpty()) {
            flushDataBlock();
        }

        if (pendingIndexBlockHandle != null) {
            indexBlock.add(lastKey + 1, pendingIndexBlockHandle.encode());
            pendingIndexBlockHandle = null;
        }

        BlockHandle indexBlockHandle = writeBlock(indexBlock);

        Footer footer = new Footer(indexBlockHandle);
        byte[] footerBytes = footer.encode();
        fileChannel.write(ByteBuffer.wrap(footerBytes));

        offset += footerBytes.length;

        return offset;
    }
}
