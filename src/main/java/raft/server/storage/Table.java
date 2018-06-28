package raft.server.storage;

import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Table {
    private FileChannel fileChannel;
    private Block indexBlock;

    private Table(FileChannel fileChannel, Block indexBlock) {
        this.fileChannel = fileChannel;
        this.indexBlock = indexBlock;
    }

    static Table open(FileChannel fileChannel, long fileSize) throws IOException {
        long footerOffset = fileSize - Footer.tableFooterSize;
        ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.tableFooterSize);
        fileChannel.read(footerBuffer, footerOffset);
        Footer footer = Footer.decode(footerBuffer.array());

        BlockHandle indexBlockHandle = footer.getIndexBlockHandle();
        Block indexBlock = readBlock(fileChannel, indexBlockHandle);

        return new Table(fileChannel, indexBlock);
    }

    private static Block readBlock(FileChannel fileChannel, BlockHandle handle) throws IOException {
        ByteBuffer content = ByteBuffer.allocate(handle.getSize());
        fileChannel.read(content, handle.getOffset());

        ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        fileChannel.read(trailer);

        long expectChecksum = trailer.getLong();
        CRC32 actualChecksum = new CRC32();
        actualChecksum.update(content.array());
        checkState(expectChecksum != actualChecksum.getValue(), "block checksum mismatch");

        return new Block(content);
    }

    List<LogEntry> getEntries(int start, int end) throws IOException {
        List<BlockHandle> indexes = indexBlock.getRangeValues(start, end)
                .stream().map(index -> {
                    BlockHandle handle = new BlockHandle();
                    handle.decode(index);
                    return handle;
                }).collect(Collectors.toList());

        List<Block> targetBlocks = new ArrayList<>();
        for (BlockHandle handle : indexes) {
            targetBlocks.add(readBlock(fileChannel, handle));
        }

        List<byte[]> entryBytes = targetBlocks.stream().map(block ->
            block.getRangeValues(start, end)
        ).flatMap(List::stream).collect(Collectors.toList());

        List<LogEntry> ret = new ArrayList<>(entryBytes.size());
        for (byte[] bs : entryBytes) {
            LogEntry e = LogEntry.parseFrom(bs);
            ret.add(e);
        }

        return ret;
    }
}
