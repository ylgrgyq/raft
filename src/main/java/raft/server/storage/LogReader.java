package raft.server.storage;

import java.io.Closeable;
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
public class LogReader implements Closeable {
    private static final ByteBuffer emptyBuffer = ByteBuffer.wrap(new byte[0]);
    private FileChannel workingFileChannel;
    private long initialOffset;
    private ByteBuffer buffer;
    private boolean eof;
    private int blockRemain;

    LogReader(FileChannel workingFileChannel) throws IOException {
        this(workingFileChannel, 0);
    }

    LogReader(FileChannel workingFileChannel, long initialOffset) throws IOException {
        this.workingFileChannel = workingFileChannel;
        long size = Math.min(workingFileChannel.size(), Integer.MAX_VALUE);
        this.buffer = workingFileChannel.map(FileChannel.MapMode.READ_ONLY, 0, size);
        this.blockRemain = Math.min(Constant.kBlockSize, buffer.remaining());
        this.initialOffset = initialOffset;
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
            blockRemain = Math.min(Constant.kBlockSize, buffer.remaining());
            workingFileChannel.position(blockStartPosition);
            long size = Math.min(workingFileChannel.size() - blockStartPosition, Integer.MAX_VALUE);
            buffer = workingFileChannel.map(FileChannel.MapMode.READ_ONLY,
                    blockStartPosition, size);
        }
    }

    private RecordType readRecord(List<byte[]> out) throws IOException {
        while (true) {
            if (blockRemain < Constant.kHeaderSize) {
                if (eof) {
                    return blockRemain > 0 ? RecordType.kUnfinished : RecordType.kEOF;
                } else {
                    if (buffer.remaining() < Constant.kBlockSize) {
                        long size = Math.min(workingFileChannel.size() - buffer.position(), Integer.MAX_VALUE);
                        buffer = workingFileChannel.map(FileChannel.MapMode.READ_ONLY,
                                    buffer.position(), size);
                    }

                    if (blockRemain > 0) {
                        // need to skip padding area
                        if (blockRemain > buffer.remaining()) {
                            return RecordType.kEOF;
                        }

                        buffer.position(buffer.position() + blockRemain);
                    }

                    blockRemain = Math.min(Constant.kBlockSize, buffer.remaining());

                    if (blockRemain < Constant.kBlockSize) {
                        eof = true;
                        continue;
                    }
                }
            }

            CRC32 actualChecksum = new CRC32();
            long expectChecksum = buffer.getLong();
            short length = buffer.getShort();

            if (length > blockRemain) {
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

            blockRemain -= length + Constant.kHeaderSize;
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

    @Override
    public void close() throws IOException {
        workingFileChannel.close();
        buffer = emptyBuffer;
    }
}
