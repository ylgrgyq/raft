package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/6/28
 */
public class TableTest {
    private static final String testingDirectory = "./target/storage";
    private static final String tableFileName = "table_test";
    private List<LogEntry> testingEntries;
    private Table testingTable;

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));

        Path p = Paths.get(testingDirectory, tableFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE);
        TableBuilder builder = new TableBuilder(ch);

        testingEntries = Collections.unmodifiableList(
                TestUtil.newLogEntryList(5000, 128, 4096));
        for (LogEntry e : testingEntries) {
            builder.add(e.getIndex(), e.toByteArray());
        }

        long tableFileSize = builder.finishBuild();
        testingTable = readTable(p, tableFileSize);
    }

    private Table readTable(Path p, long expectTableSize) throws IOException{
        byte[] content = Files.readAllBytes(p);
        assertEquals(expectTableSize, content.length);

        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        return Table.open(ch, expectTableSize);
    }

    @Test
    public void getEntries() throws Exception {
        long firstIndex = testingEntries.get(0).getIndex();
        long lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        long cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = testingTable.getEntries(cursor, cursor + step);
            List<LogEntry> expect = testingEntries.subList((int)(cursor - firstIndex), Math.min((int)(cursor + step - firstIndex), testingEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                assertEquals(expect.get(i), actual.get(i));
            }

            cursor += step;
        }
    }

    @Test
    public void getEntriesWithOutOfRangeKeys() throws Exception {
        long firstIndex = testingEntries.get(0).getIndex();
        long lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        long cursor = ThreadLocalRandom.current().nextLong(1L, firstIndex);
        List<LogEntry> actual = testingTable.getEntries(cursor, lastIndex + 1000);
        assertEquals(testingEntries.size(), actual.size());
        for (int i = 0; i < testingEntries.size(); i++) {
            assertEquals(testingEntries.get(i), actual.get(i));
        }
    }
}
