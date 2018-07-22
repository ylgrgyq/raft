package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/6/27
 */
public class BlockTest {
    private static final String testingDirectory = "./target/storage";
    private static final String blockFileName = "block_test";
    private static Block testingBlock;
    private static List<LogEntry> testingEntries;

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));

        BlockBuilder builder = new BlockBuilder();

        testingEntries = TestUtil.newLogEntryList(10000, 128, 2048);
        for (LogEntry e : testingEntries) {
            builder.add(e.getIndex(), e.toByteArray());
        }

        Path p = Paths.get(testingDirectory, blockFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);

        long expectChecksum = builder.writeBlock(ch);
        int blockSize = builder.getBlockSize();

        testingBlock = readBlock(p, blockSize, expectChecksum);
    }

    private Block readBlock(Path p, int expectBlockSize, long expectChecksum) throws IOException{
        byte[] content = Files.readAllBytes(p);
        assertEquals(expectBlockSize, content.length);

        ByteBuffer buffer = ByteBuffer.wrap(content);
        CRC32 actualChecksum = new CRC32();
        actualChecksum.update(buffer.array());
        assertEquals(expectChecksum, actualChecksum.getValue());

        return new Block(buffer);
    }

    @Test
    public void getEntries() throws Exception {
        int firstIndex = testingEntries.get(0).getIndex();
        int lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        int cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<byte[]> actual = testingBlock.getValuesByKeyRange(cursor, cursor + step);
            List<LogEntry> expect = testingEntries.subList(cursor - firstIndex, Math.min(cursor + step - firstIndex, testingEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                assertEquals(expect.get(i), LogEntry.parseFrom(actual.get(i)));
            }

            cursor += step;
        }
    }

    @Test
    public void getEntriesWithOutOfRangeKeys() throws Exception {
        int firstIndex = testingEntries.get(0).getIndex();
        int lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        int cursor = ThreadLocalRandom.current().nextInt(1, firstIndex);
        List<LogEntry> actual = testingBlock.getValuesByKeyRange(cursor, lastIndex + 1000)
                .stream()
                .map(bs -> {
                    try {
                        return LogEntry.parseFrom(bs);
                    }catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                })
                .collect(Collectors.toList());
        assertEquals(testingEntries.size(), actual.size());
        for (int i = 0; i < testingEntries.size(); i++) {
            assertEquals(testingEntries.get(i), actual.get(i));
        }
    }
}