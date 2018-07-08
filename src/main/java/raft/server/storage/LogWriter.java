package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class LogWriter {
    private final FileChannel workingFileChannel;
    private int blockOffset;

    LogWriter(FileChannel workingFileChannel) {
        assert workingFileChannel != null;

        this.workingFileChannel = workingFileChannel;
        this.blockOffset = 0;
    }

    LogWriter(FileChannel workingFileChannel, long writePosotion) throws IOException {
        assert workingFileChannel != null;

        workingFileChannel.position(writePosotion);
        this.workingFileChannel = workingFileChannel;
        this.blockOffset = 0;
    }

    long getPosition() throws IOException{
        return workingFileChannel.position();
    }

    void flush() throws IOException{
        workingFileChannel.force(true);
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

        if (blockLeft > 0) {
            // padding with bytes array full of zero
            ByteBuffer buffer = ByteBuffer.allocate(blockLeft);
            workingFileChannel.write(buffer);
        }
    }

    private void writeRecord(RecordType type, byte[] out) throws IOException{
        assert blockOffset + Constant.kHeaderSize + out.length <= Constant.kBlockSize;

        ByteBuffer buffer = ByteBuffer.allocate(Constant.kHeaderSize);
        CRC32 checksum = new CRC32();
        checksum.update(type.getCode());
        checksum.update(out);
        buffer.putLong(checksum.getValue());
        buffer.putShort((short) out.length);
        buffer.put(type.getCode());
        buffer.flip();
        workingFileChannel.write(buffer);
        workingFileChannel.write(ByteBuffer.wrap(out));
        blockOffset += out.length + Constant.kHeaderSize;
    }
}
