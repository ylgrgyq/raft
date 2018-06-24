package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
public class TableBuilder {
    private FileChannel fileChannel;
    private BlockBuilder dataBlock;
    private BlockBuilder indexBlock;
    private BlockHandle pendingIndexBlockHandle;
    private int lastKey = -1;
    private long offset;

    TableBuilder(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
        dataBlock = new BlockBuilder();
        indexBlock = new BlockBuilder();
    }

    public void add(int k, byte[] v) throws IOException {
        if (pendingIndexBlockHandle != null) {
            indexBlock.add(k, pendingIndexBlockHandle.encode());
            pendingIndexBlockHandle = null;
        }

        long blockSize = dataBlock.add(k, v);

        if (blockSize > Constant.kMaxBlockSize) {
            flushDataBlock();
        }

        lastKey = k;
    }

    private void flushDataBlock() throws IOException {
        pendingIndexBlockHandle = new BlockHandle();
        writeBlock(pendingIndexBlockHandle, dataBlock);
        fileChannel.force(true);
    }

    private void writeBlock(BlockHandle handle, BlockBuilder block) throws IOException{
        handle.setOffset(offset);
        handle.setSize(block.getBlockSize());

        CRC32 checksum = new CRC32();
        for(byte[] data : block.getBlockContents()) {
            checksum.update(data);
            fileChannel.write(ByteBuffer.wrap(data));
        }

        ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        trailer.putLong(checksum.getValue());
        trailer.flip();
        fileChannel.write(trailer);

        dataBlock.reset();
        offset += dataBlock.getBlockSize() + Constant.kBlockTrailerSize;
    }

    public void build() throws IOException {
        flushDataBlock();

        if (pendingIndexBlockHandle != null) {
            indexBlock.add(lastKey + 1, pendingIndexBlockHandle.encode());
            pendingIndexBlockHandle = null;
        }

        BlockHandle indexBlockHandle = new BlockHandle();
        writeBlock(indexBlockHandle, indexBlock);

        Footer footer = new Footer(indexBlockHandle);
        byte[] footerBytes = footer.encode();
        fileChannel.write(ByteBuffer.wrap(footerBytes));

        offset += footerBytes.length;
    }

    long getFileSize() {
        return offset;
    }

    private static class BlockHandle {
        private long offset;
        private long size;

        public void setOffset(long offset) {
            this.offset = offset;
        }

        public void setSize(long size) {
            this.size = size;
        }

        byte[] encode() {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
            buffer.putLong(offset);
            buffer.putLong(size);

            return buffer.array();
        }
    }

    private static class Footer {
        private BlockHandle indexBlockHandle;

        public Footer(BlockHandle indexBlockHandle) {
            this.indexBlockHandle = indexBlockHandle;
        }

        byte[] encode() {
            byte[] indexBlock = indexBlockHandle.encode();
            ByteBuffer buffer = ByteBuffer.allocate(indexBlock.length + Long.BYTES);
            buffer.put(indexBlock);
            buffer.putLong(Constant.kTableMagicNumber);

            return buffer.array();
        }
    }
}
