package raft.server.storage;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
public class LogWriterWithMmap implements Closeable {
    private final FileChannel workingFileChannel;
    private int blockOffset;
    private MappedByteBuffer buffer;
    private long bufferStartPos;

    LogWriterWithMmap(FileChannel workingFileChannel) throws IOException {
        assert workingFileChannel != null;

        this.workingFileChannel = workingFileChannel;
        this.blockOffset = 0;
        bufferStartPos = 0;
        this.buffer = workingFileChannel.map(FileChannel.MapMode.READ_WRITE, bufferStartPos, Integer.MAX_VALUE);
    }

    LogWriterWithMmap(FileChannel workingFileChannel, long writePosotion) throws IOException {
        assert workingFileChannel != null;

        workingFileChannel.position(writePosotion);
        this.workingFileChannel = workingFileChannel;
        this.blockOffset = 0;
    }

    long getPosition() {
        return bufferStartPos + buffer.position();
    }

    void flush() {
        buffer.force();
    }

    @Override
    public void close() throws IOException {
        long pos = bufferStartPos + buffer.position();
        buffer.force();
        buffer = null;
        workingFileChannel.truncate(pos);
        workingFileChannel.close();
    }

    void append(byte[] data) throws IOException{
        assert data != null;
        assert data.length > 0;

        ByteBuffer writeBuffer = ByteBuffer.wrap(data);
        int dataSizeRemain = writeBuffer.remaining();
        boolean begin = true;

        while (dataSizeRemain > 0) {
            int blockLeft = Constant.kBlockSize - blockOffset;

            if (blockLeft < Constant.kHeaderSize) {
                paddingBlock(blockLeft);
                blockOffset = 0;
                continue;
            }

            assert Constant.kBlockSize - blockOffset - Constant.kHeaderSize >= 0;

            final RecordType type;
            final int blockForDataAvailable = blockLeft - Constant.kHeaderSize;
            final int fragmentSize = Math.min(blockForDataAvailable, dataSizeRemain);
            final boolean end = fragmentSize == dataSizeRemain;
            if (begin && end) {
                type = RecordType.kFullType;
            } else if (begin) {
                type = RecordType.kFirstType;
            } else if (end) {
                type = RecordType.kLastType;
            } else {
                type = RecordType.kMiddleType;
            }

            byte[] out = new byte[fragmentSize];
            writeBuffer.get(out);
            writeRecord(type, out);

            begin = false;
            dataSizeRemain -= fragmentSize;
        }
    }

    private void paddingBlock(int blockLeft) throws IOException{
        assert blockLeft >= 0 : String.format("blockLeft:%s", blockLeft);

        if (buffer.remaining() < blockLeft) {
            bufferStartPos = buffer.position();
            buffer = workingFileChannel.map(FileChannel.MapMode.READ_WRITE, bufferStartPos, Integer.MAX_VALUE);
        }

        while (blockLeft > 0) {
            // padding with bytes array full of zero
            this.buffer.put((byte)0);
            --blockLeft;
        }
    }

    private void writeRecord(RecordType type, byte[] blockPayload) throws IOException {
        assert blockOffset + Constant.kHeaderSize + blockPayload.length <= Constant.kBlockSize;

        if (buffer.remaining() < Constant.kHeaderSize + blockPayload.length) {
            bufferStartPos = buffer.position();
            buffer = workingFileChannel.map(FileChannel.MapMode.READ_WRITE, bufferStartPos, Integer.MAX_VALUE);
        }

        CRC32 checksum = new CRC32();
        checksum.update(type.getCode());
        checksum.update(blockPayload);
        buffer.putLong(checksum.getValue());
        buffer.putShort((short) blockPayload.length);
        buffer.put(type.getCode());
        buffer.put(blockPayload);
        blockOffset += blockPayload.length + Constant.kHeaderSize;
    }
}