package raft.server.storage;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class BlockHandle {
    static int blockHandleSize = Long.BYTES + Long.BYTES;

    private long offset;
    private long size;

    void setOffset(long offset) {
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

    void decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        offset = buffer.getLong();
        size = buffer.getLong();
    }
}

