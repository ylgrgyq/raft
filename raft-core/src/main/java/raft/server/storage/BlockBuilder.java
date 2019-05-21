package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class BlockBuilder {
    private final List<ByteBuffer> buffers;
    private final List<Integer> checkPoints;
    private int blockSize;
    private int entryCounter;
    private boolean isBuilt;

    BlockBuilder() {
        buffers = new ArrayList<>();
        checkPoints = new ArrayList<>();
    }

    long add(long k, byte[] v) {
        assert !isBuilt;
        assert v.length > 0 : String.format("actual:%s", v.length);
        assert entryCounter >= 0 && entryCounter < Integer.MAX_VALUE;

        if ((entryCounter++ & (Constant.kBlockCheckpointInterval - 1)) == 0) {
            checkPoints.add(blockSize);
        }

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + v.length);
        buffer.putLong(k);
        buffer.putInt(v.length);
        buffer.put(v);
        buffer.flip();

        buffers.add(buffer);
        blockSize += buffer.limit();

        return blockSize;
    }

    int getCurrentEstimateBlockSize() {
        return blockSize
                // checkpoints
                + Integer.BYTES * checkPoints.size()
                // checkpoints count
                + Integer.BYTES;
    }

    long writeBlock(FileChannel fileChannel) throws IOException {
        assert !isBuilt;
        assert fileChannel != null;
        assert !buffers.isEmpty();
        assert !checkPoints.isEmpty();

        isBuilt = true;

        // append checkpoints
        ByteBuffer checkpointsBuffer = ByteBuffer.allocate(Integer.BYTES * checkPoints.size() + Integer.BYTES);
        for (Integer checkpoint : checkPoints) {
            checkpointsBuffer.putInt(checkpoint);
        }
        checkpointsBuffer.putInt(checkPoints.size());
        checkpointsBuffer.flip();
        buffers.add(checkpointsBuffer);
        blockSize += checkpointsBuffer.limit();

        // write whole block include block and checkpoints
        CRC32 checksum = new CRC32();
        for (ByteBuffer buffer : buffers) {
            checksum.update(buffer.array());
        }
        ByteBuffer[] bufferArray = new ByteBuffer[buffers.size()];
        buffers.toArray(bufferArray);

        // maybe we have a lot of buffers which may not be written to channel at once, so we need loop and
        // tracking how many bytes we have written
        long written = 0;
        while (written < blockSize) {
            written += fileChannel.write(bufferArray);
        }

        assert written > 0;
        return checksum.getValue();
    }

    int getBlockSize() {
        return blockSize;
    }

    boolean isEmpty() {
        return blockSize == 0;
    }

    void reset() {
        buffers.clear();
        checkPoints.clear();
        blockSize = 0;
        entryCounter = 0;
        isBuilt = false;
    }
}
