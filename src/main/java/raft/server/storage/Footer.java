package raft.server.storage;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class Footer {
    static int tableFooterSize = BlockHandle.blockHandleSize;

    private BlockHandle indexBlockHandle;

    Footer(BlockHandle indexBlockHandle) {
        this.indexBlockHandle = indexBlockHandle;
    }

    BlockHandle getIndexBlockHandle() {
        return indexBlockHandle;
    }

    byte[] encode() {
        byte[] indexBlock = indexBlockHandle.encode();
        ByteBuffer buffer = ByteBuffer.allocate(indexBlock.length + Long.BYTES);
        buffer.put(indexBlock);
        buffer.putLong(Constant.kTableMagicNumber);

        return buffer.array();
    }

    static Footer decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        byte[] indexBlockHandleBytes = new byte[BlockHandle.blockHandleSize];
        buffer.get(indexBlockHandleBytes);

        long magic = buffer.getLong();
        if (magic != Constant.kTableMagicNumber) {
            throw new IllegalStateException("found invalid sstable during checking magic number");
        }

        BlockHandle indexBlockHandle = new BlockHandle();
        indexBlockHandle.decode(indexBlockHandleBytes);
        return new Footer(indexBlockHandle);
    }
}
