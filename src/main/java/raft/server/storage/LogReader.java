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
    private static final byte[] empty = new byte[0];
    private FileChannel workingFileChannel;
    private long initialOffset;
    private ByteBuffer buffer;

    LogReader(FileChannel workingFileChannel) {
        this(workingFileChannel, 0);
    }

    // TODO recovery from encountering bad record instead of always throws exception
    LogReader(FileChannel workingFileChannel, long initialOffset) {
        this.workingFileChannel = workingFileChannel;
        this.initialOffset = initialOffset;
        this.buffer = ByteBuffer.wrap(empty);
    }

    Optional<byte[]> readLog() throws IOException {
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
                case kEOF:
                    Optional.empty();
                    break;
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
        boolean eof = false;
        while (true) {
            if (buffer.remaining() < Constant.kHeaderSize) {
                if (eof) {
                    // TODO we may have remaining buffer but eof == true which means log writer may died in the middle of
                    // writing this record's header, we need to handle this like fix this file
                    buffer = ByteBuffer.wrap(empty);
                    return RecordType.kEOF;
                } else {
                    buffer = ByteBuffer.allocate(Constant.kBlockSize);
                    int readBytes = workingFileChannel.read(buffer);
                    if (readBytes < Constant.kBlockSize) {
                        eof = true;
                    }
                    continue;
                }
            }

            CRC32 actualChecksum = new CRC32();
            long expectChecksum = buffer.getLong();
            short length = buffer.getShort();

            if (Constant.kHeaderSize + length > buffer.remaining()) {
                if (eof) {
                    // TODO writer died in the middle of writing this record, we need to allow this situation
                    buffer = ByteBuffer.wrap(empty);
                    return RecordType.kEOF;
                }
                // buffer underflow throw exception on read data from buffer
            }

            byte typeCode = buffer.get();
            actualChecksum.update(typeCode);
            RecordType type = RecordType.getRecordTypeByCode(typeCode);
            byte[] buf = new byte[length];
            buffer.get(buf);
            actualChecksum.update(buf);

            if (actualChecksum.getValue() != expectChecksum) {
                buffer = ByteBuffer.wrap(empty);
                throw new IllegalStateException();
            }

            out.add(buf);
            return type;
        }
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
