package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.zip.CRC32;

import static com.google.common.base.Preconditions.checkState;
import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/6/27
 */
public class BlockTest {
    private static final String testingDirectory = "./target/storage";
    private static final String blockFileName = "block_test";

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));
    }

    @Test
    public void testBuildAndReadBlock() throws Exception {
        BlockBuilder builder = new BlockBuilder();

        List<LogEntry> entries = TestUtil.newLogEntryList(10000, 128, 2048);
        for (LogEntry e : entries) {
            builder.add(e.getIndex(), e.toByteArray());
        }

        Path p = Paths.get(testingDirectory, blockFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        long expectChecksum = builder.writeBlock(ch);
        int blockSize = builder.getBlockSize();

        byte[] content = Files.readAllBytes(p);
        assertEquals(blockSize, content.length);

        ByteBuffer buffer = ByteBuffer.wrap(content);
        CRC32 actualChecksum = new CRC32();
        actualChecksum.update(buffer.array());
        assertEquals(expectChecksum, actualChecksum.getValue());

        Block block = new Block(buffer);
        int firstIndex = entries.get(0).getIndex();
        int lastIndex = entries.get(entries.size() - 1).getIndex();
        int cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<byte[]> actual = block.getValuesByKeyRange(cursor, cursor + step);
            List<LogEntry> expect = entries.subList(cursor - firstIndex, Math.min(cursor + step - firstIndex, entries.size()));
            for (int i = 0; i < expect.size(); i++) {
                assertEquals(expect.get(i), LogEntry.parseFrom(actual.get(i)));
            }

            cursor += step;
        }
    }
}