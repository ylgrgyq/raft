package raft.server.storage;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class Footer {
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

    void decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] indexBlockHandleBytes = new byte[BlockHandle.blockHandleSize];
        buffer.get(indexBlockHandleBytes);

        long magic = buffer.getLong();
        if (magic != Constant.kTableMagicNumber) {
            throw new IllegalStateException("found invalid sstable during checking magic number");
        }

        indexBlockHandle = new BlockHandle();
        indexBlockHandle.decode(indexBlockHandleBytes);
    }
}
