package raft.server.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Table {
    private FileChannel fileChannel;
    private Block indexBlock;

    Table(FileChannel fileChannel, Block indexBlock) {
        this.fileChannel = fileChannel;
        this.indexBlock = indexBlock;
    }

    static Table open(FileChannel fileChannel, long fileSize) throws IOException{
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
}
