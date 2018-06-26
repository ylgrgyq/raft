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
    private List<ByteBuffer> buffers;
    private int blockSize;
    private List<Integer> checkPoints;
    private int entryCounter;
    private boolean isBuilt;

    BlockBuilder() {
        this.buffers = new ArrayList<>();
        this.checkPoints = new ArrayList<>();
    }

    long add(int k, byte[] v) {
        assert !isBuilt;

        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + v.length);
        buffer.putInt(k);
        buffer.putInt(v.length);
        buffer.put(v);
        buffer.flip();

        buffers.add(buffer);
        blockSize += buffer.limit();
        entryCounter++;

        if ((++entryCounter & Constant.kBlockCheckpointInterval - 1) == 0) {
            checkPoints.add(blockSize);
        }

        return blockSize;
    }

    int getCurrentEstimateBlockSize() {
        return blockSize +
                Integer.BYTES * checkPoints.size()
                + Integer.BYTES;
    }

    int writeBlock(FileChannel fileChannel) throws IOException{
        assert ! isBuilt;
        isBuilt = true;

        CRC32 checksum = new CRC32();

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
        for(ByteBuffer buffer : buffers) {
            checksum.update(buffer.array());
        }
        ByteBuffer[] bufferArray = new ByteBuffer[buffers.size()];
        buffers.toArray(bufferArray);
        fileChannel.write(bufferArray);

        // write tailer
        ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        trailer.putLong(checksum.getValue());
        trailer.flip();
        fileChannel.write(trailer);

        return blockSize + Constant.kBlockTrailerSize;
    }

    void reset() {
        buffers.clear();
        checkPoints.clear();
        blockSize = 0;
        entryCounter = 0;
        isBuilt = false;
    }
}
