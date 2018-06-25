package raft.server.storage;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class BlockHandle {
    static int blockHandleSize = Long.BYTES + Integer.BYTES;

    private long offset;
    private int size;

    void setOffset(long offset) {
        this.offset = offset;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public long getOffset() {
        return offset;
    }

    public int getSize() {
        return size;
    }

    byte[] encode() {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
        buffer.putLong(offset);
        buffer.putInt(size);

        return buffer.array();
    }

    void decode(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        offset = buffer.getLong();
        size = buffer.getInt();
    }
}

