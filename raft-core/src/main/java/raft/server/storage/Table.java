package raft.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import raft.server.log.StorageInternalError;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

import static raft.server.util.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Table implements Iterable<LogEntry> {
    private final Cache<Long, Block> dataBlockCache = CacheBuilder.newBuilder()
            .initialCapacity(1024)
            .maximumSize(2048)
            .build();

    private final FileChannel fileChannel;
    private final Block indexBlock;

    private Table(FileChannel fileChannel, Block indexBlock) {
        this.fileChannel = fileChannel;
        this.indexBlock = indexBlock;
    }

    static Table open(FileChannel fileChannel, long fileSize) throws IOException {
        long footerOffset = fileSize - Footer.tableFooterSize;
        ByteBuffer footerBuffer = ByteBuffer.allocate(Footer.tableFooterSize);
        fileChannel.read(footerBuffer, footerOffset);
        footerBuffer.flip();
        Footer footer = Footer.decode(footerBuffer.array());

        BlockHandle indexBlockHandle = footer.getIndexBlockHandle();
        Block indexBlock = readBlock(fileChannel, indexBlockHandle);

        return new Table(fileChannel, indexBlock);
    }

    void close() throws IOException {
        fileChannel.close();
        dataBlockCache.invalidateAll();
    }

    private static Block readBlock(FileChannel fileChannel, BlockHandle handle) throws IOException {
        ByteBuffer content = ByteBuffer.allocate(handle.getSize());
        fileChannel.read(content, handle.getOffset());

        ByteBuffer trailer = ByteBuffer.allocate(Constant.kBlockTrailerSize);
        fileChannel.read(trailer);
        trailer.flip();

        long expectChecksum = trailer.getLong();
        CRC32 actualChecksum = new CRC32();
        actualChecksum.update(content.array());
        checkArgument(expectChecksum != actualChecksum.getValue(), "block checksum mismatch");

        return new Block(content);
    }

    List<LogEntry> getEntries(long start, long end) {
        SeekableIterator<Long, LogEntry> itr = iterator();
        itr.seek(start);

        List<LogEntry> ret = new ArrayList<>();
        while (itr.hasNext()) {
            LogEntry v = itr.next();
            long k = v.getIndex();
            if (k >= start && k < end) {
                ret.add(v);
            } else {
                break;
            }
        }

        return ret;
    }

    private Block getBlock(BlockHandle handle) throws IOException {
        Block block = dataBlockCache.getIfPresent(handle.getOffset());
        if (block == null) {
            block = readBlock(fileChannel, handle);
            dataBlockCache.put(handle.getOffset(), block);
        }

        return block;
    }

    @Override
    public SeekableIterator<Long, LogEntry> iterator() {
        return new Itr(indexBlock);
    }

    private class Itr implements SeekableIterator<Long, LogEntry> {
        private final SeekableIterator<Long, KeyValueEntry<Long, byte[]>> indexBlockIter;
        private SeekableIterator<Long, KeyValueEntry<Long, byte[]>> innerBlockIter;

        Itr(Block indexBlock) {
            this.indexBlockIter = indexBlock.iterator();
        }

        @Override
        public void seek(Long key) {
            indexBlockIter.seek(key);
            if (indexBlockIter.hasNext()) {
                innerBlockIter = createInnerBlockIter();
                innerBlockIter.seek(key);
            } else {
                innerBlockIter = null;
            }
        }

        @Override
        public boolean hasNext() {
            if (innerBlockIter == null || !innerBlockIter.hasNext()) {
                if (indexBlockIter.hasNext()) {
                    innerBlockIter = createInnerBlockIter();
                }
            }

            return innerBlockIter != null && innerBlockIter.hasNext();
        }

        private SeekableIterator<Long, KeyValueEntry<Long,byte[]>> createInnerBlockIter() {
            try {
                KeyValueEntry<Long, byte[]> kv = indexBlockIter.next();
                BlockHandle handle = BlockHandle.decode(kv.getVal());
                Block block = getBlock(handle);
                return block.iterator();
            } catch (IOException ex) {
                throw new StorageInternalError("create inner block iterator failed", ex);
            }
        }

        @Override
        public LogEntry next() {
            assert innerBlockIter != null;
            assert innerBlockIter.hasNext();
            try {
                KeyValueEntry<Long, byte[]> ret = innerBlockIter.next();
                return LogEntry.parseFrom(ret.getVal());
            } catch (IOException ex) {
                throw new StorageInternalError(ex);
            }
        }
    }
}
