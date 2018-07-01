package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));
    }

    @Test
    public void testBuildAndReadTable() throws Exception {
        Path p = Paths.get(testingDirectory, tableFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        TableBuilder builder = new TableBuilder(ch);

        List<LogEntry> entries = TestUtil.newLogEntryList(5000, 128, 4096);
        for (LogEntry e : entries) {
            builder.add(e.getIndex(), e.toByteArray());
        }

        long tableFileSize = builder.finishBuild();
        byte[] content = Files.readAllBytes(p);
        assertEquals(tableFileSize, content.length);

        ch = FileChannel.open(p, StandardOpenOption.READ);
        Table table = Table.open(ch, tableFileSize);

        int firstIndex = entries.get(0).getIndex();
        int lastIndex = entries.get(entries.size() - 1).getIndex();
        int cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = table.getEntries(cursor, cursor + step);
            List<LogEntry> expect = entries.subList(cursor - firstIndex, Math.min(cursor + step - firstIndex, entries.size()));
            for (int i = 0; i < expect.size(); i++) {
                assertEquals(expect.get(i), actual.get(i));
            }

            cursor += step;
        }
    }
}
