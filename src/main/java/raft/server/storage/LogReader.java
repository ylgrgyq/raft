package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class LogReader {
    private static final ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);
    private FileChannel workingFileChannel;
    private long initialOffset;
    private ByteBuffer buffer;
    private boolean eof;

    LogReader(FileChannel workingFileChannel) {
        this(workingFileChannel, 0);
    }

    LogReader(FileChannel workingFileChannel, long initialOffset) {
        this.workingFileChannel = workingFileChannel;
        this.initialOffset = initialOffset;
        this.buffer = emptyBuffer;
    }

    Optional<byte[]> readLog() throws IOException, BadRecordException {
        if (initialOffset > 0) {
            skipToInitBlock();
        }

        boolean isFragmented = false;
        ArrayList<byte[]> outPut = new ArrayList<>();
        while (true) {
            RecordType type = readRecord(outPut);
            switch (type) {
                case kFullType:
                    if (isFragmented) {
                        throw new IllegalStateException();
                    }
                    return Optional.of(compact(outPut));
                case kFirstType:
                    if (isFragmented) {
                        throw new IllegalStateException();
                    }
                    isFragmented = true;
                    break;
                case kMiddleType:
                    if (!isFragmented) {
                        throw new IllegalStateException();
                    }
                    break;
                case kLastType:
                    if (!isFragmented) {
                        throw new IllegalStateException();
                    }
                    return Optional.of(compact(outPut));
                case kCorruptedRecord:
                case kUnfinished:
                    buffer = emptyBuffer;
                    throw new BadRecordException(type);
                case kEOF:
                    buffer = emptyBuffer;
                    return Optional.empty();
            }
        }
    }

    private void skipToInitBlock() throws IOException {
        long offsetInBlock = initialOffset % Constant.kBlockSize;
        long blockStartPosition = initialOffset - offsetInBlock;

        // if remaining space in block can not write a whole header, log writer
        // will write empty buffer to pad that space. so we should check if
        // offsetInBlock is within padding area and forward blockStartPosition
        // to the start position of the next real block
        if (offsetInBlock > Constant.kBlockSize - Constant.kHeaderSize + 1) {
            blockStartPosition += Constant.kBlockSize;
        }

        if (blockStartPosition > 0) {
            workingFileChannel.position(blockStartPosition);
        }
    }

    private RecordType readRecord(List<byte[]> out) throws IOException {
        outer:
        if (buffer.remaining() < Constant.kHeaderSize) {
            if (eof) {
                return buffer.remaining() > 0 ? RecordType.kUnfinished : RecordType.kEOF;
            } else {
                buffer = ByteBuffer.allocate(Constant.kBlockSize);
                while (buffer.hasRemaining()) {
                    int readBs = workingFileChannel.read(buffer);
                    if (readBs == -1) {
                        eof = true;
                        buffer.flip();
                        break outer;
                    }
                }
                buffer.flip();
            }
        }

        CRC32 actualChecksum = new CRC32();
        long expectChecksum = buffer.getLong();
        short length = buffer.getShort();

        if (length > buffer.remaining()) {
            return eof ? RecordType.kUnfinished : RecordType.kCorruptedRecord;
        }

        byte typeCode = buffer.get();
        actualChecksum.update(typeCode);
        RecordType type = RecordType.getRecordTypeByCode(typeCode);
        byte[] buf = new byte[length];
        buffer.get(buf);
        actualChecksum.update(buf);

        if (actualChecksum.getValue() != expectChecksum) {
            return RecordType.kCorruptedRecord;
        }

        out.add(buf);
        return type;
    }

    // TODO find some way to avoid copy bytes
    private byte[] compact(List<byte[]> output) {
        int size = output.stream().mapToInt(b -> b.length).sum();
        ByteBuffer buffer = ByteBuffer.allocate(size);
        for (byte[] bytes : output) {
            buffer.put(bytes);
        }
        return buffer.array();
    }
}
