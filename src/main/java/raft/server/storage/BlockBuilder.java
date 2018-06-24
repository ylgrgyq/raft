package raft.server.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class BlockBuilder {
    private List<byte[]> buffers;
    private long blockSize;


    BlockBuilder() {
        this.buffers = new ArrayList<>();
    }

    long add(int k, byte[] v) {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + v.length);
        buffer.putInt(k);
        buffer.putInt(v.length);
        buffer.put(v);

        buffers.add(buffer.array());
        blockSize += buffer.position();

        return blockSize;
    }

    long getBlockSize() {
        return blockSize;
    }

    List<byte[]> getBlockContents() {
        return buffers;
    }

    void reset() {
        buffers.clear();
        blockSize = 0;
    }
}
