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
        this.fileChannel = fileChannel;
        dataBlock = new BlockBuilder();
        indexBlock = new BlockBuilder();
    }

    void add(int k, byte[] v) throws IOException {
        assert k > lastKey;
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
        offset += blockSize;

        return handle;
    }

    void finishBuild() throws IOException {
        assert ! isFinished;

        isFinished = true;

        flushDataBlock();

        if (pendingIndexBlockHandle != null) {
            indexBlock.add(lastKey + 1, pendingIndexBlockHandle.encode());
            pendingIndexBlockHandle = null;
        }

        BlockHandle indexBlockHandle = writeBlock(indexBlock);

        Footer footer = new Footer(indexBlockHandle);
        byte[] footerBytes = footer.encode();
        fileChannel.write(ByteBuffer.wrap(footerBytes));

        offset += footerBytes.length;
    }

    long getFileSize() {
        return offset;
    }
}
