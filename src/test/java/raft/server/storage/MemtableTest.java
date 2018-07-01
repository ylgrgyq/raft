package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/7/1
 */
public class MemtableTest {
    private List<LogEntry> testingEntries;
    private Memtable testingMm;

    @Before
    public void setUp() throws Exception {
        testingMm = new Memtable();
        assertTrue(testingMm.isEmpty());
        testingEntries = TestUtil.newLogEntryList(10000, 128, 2048);
        for (LogEntry e : testingEntries) {
            testingMm.add(e.getIndex(), e);
            assertTrue(!testingMm.isEmpty());
        }
    }

    @Test
    public void firstKey() throws Exception {
        assertEquals(testingEntries.get(0).getIndex(), testingMm.firstKey());
    }

    @Test
    public void lastKey() throws Exception {
        assertEquals(testingEntries.get(testingEntries.size() - 1).getIndex(), testingMm.lastKey());
    }

    @Test
    public void get() throws Exception {
        for (LogEntry expect : testingEntries) {
            assertEquals(expect, testingMm.get(expect.getIndex()));
        }
    }

    @Test
    public void getEntries() throws Exception {
        int firstIndex = testingEntries.get(0).getIndex();
        int lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        int cursor = firstIndex;
        SeekableIterator<LogEntry> mmIter = testingMm.iterator();
        mmIter.seek(firstIndex);

        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = testingMm.getEntries(cursor, cursor + step);
            List<LogEntry> expect = testingEntries.subList(cursor - firstIndex,
                    Math.min(cursor + step - firstIndex, testingEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                assertEquals(expect.get(i), actual.get(i));
            }

            cursor += step;
        }
    }

    @Test
    public void getEntriesWithOutOfRangeKeys() throws Exception {
        int firstIndex = testingEntries.get(0).getIndex();
        int lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        int cursor = ThreadLocalRandom.current().nextInt(1, firstIndex);

        List<LogEntry> actual = testingMm.getEntries(cursor, lastIndex + 100000);
        assertEquals(testingEntries.size(), actual.size());
        for (int i = 0; i < testingEntries.size(); i++) {
            assertEquals(testingEntries.get(i), actual.get(i));
        }
    }

    @Test
    public void forEach() throws Exception {
        int firstIndex = testingMm.firstKey();
        for (LogEntry actual : testingMm) {
            LogEntry expect = testingEntries.get(actual.getIndex() - firstIndex);
            assertEquals(expect, actual);
        }

    }
}