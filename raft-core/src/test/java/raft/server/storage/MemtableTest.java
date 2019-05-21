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
        assertEquals((Long)testingEntries.get(0).getIndex(), testingMm.firstKey());
    }

    @Test
    public void lastKey() throws Exception {
        assertEquals((Long)testingEntries.get(testingEntries.size() - 1).getIndex(), testingMm.lastKey());
    }

    @Test
    public void get() throws Exception {
        for (LogEntry expect : testingEntries) {
            assertEquals(expect, testingMm.get(expect.getIndex()));
        }
    }

    @Test
    public void getEntries() throws Exception {
        long firstIndex = testingEntries.get(0).getIndex();
        long lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        long cursor = firstIndex;
        SeekableIterator<Long, LogEntry> mmIter = testingMm.iterator();
        mmIter.seek(firstIndex);

        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = testingMm.getEntries(cursor, cursor + step);
            List<LogEntry> expect = testingEntries.subList((int)(cursor - firstIndex),
                    Math.min((int)(cursor + step - firstIndex), testingEntries.size()));
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
        long cursor = ThreadLocalRandom.current().nextLong(1, firstIndex);

        List<LogEntry> actual = testingMm.getEntries(cursor, lastIndex + 100000);
        assertEquals(testingEntries.size(), actual.size());
        for (int i = 0; i < testingEntries.size(); i++) {
            assertEquals(testingEntries.get(i), actual.get(i));
        }
    }

    @Test
    public void forEach() throws Exception {
        long firstIndex = testingMm.firstKey();
        for (LogEntry actual : testingMm) {
            LogEntry expect = testingEntries.get((int)(actual.getIndex() - firstIndex));
            assertEquals(expect, actual);
        }

    }

    @Test
    public void overwrite() throws Exception {
        LogEntry overwritingStartEntry = testingEntries.get(testingEntries.size() / 2);
        List<LogEntry> expectRemainEntries = testingEntries.subList(0, testingEntries.size() / 2);

        List<LogEntry> overwritingEntries = TestUtil.newLogEntryList(10000, 128,
                2048, overwritingStartEntry.getTerm(), overwritingStartEntry.getIndex(),
                1, 1);
        for (LogEntry e : overwritingEntries) {
            testingMm.add(e.getIndex(), e);
        }

        long firstIndex = testingEntries.get(0).getIndex();
        long lastIndex = testingEntries.get(testingEntries.size() - 1).getIndex();
        long cursor = ThreadLocalRandom.current().nextLong(1, firstIndex);

        List<LogEntry> actual = testingMm.getEntries(cursor, lastIndex + 100000);
        int expectTotalSize = expectRemainEntries.size() + overwritingEntries.size();
        assertEquals(expectTotalSize, actual.size());
        int expectMemSize = 0;
        for (int i = 0; i < expectRemainEntries.size(); i++) {
            LogEntry actualE = actual.get(i);
            expectMemSize += Long.BYTES + actualE.getSerializedSize();
            assertEquals(expectRemainEntries.get(i), actualE);
        }

        for(int i = expectRemainEntries.size(); i < expectTotalSize; ++i) {
            LogEntry actualE = actual.get(i);
            expectMemSize += Long.BYTES + actualE.getSerializedSize();
            assertEquals(overwritingEntries.get(i - expectRemainEntries.size()), actualE);
        }

        assertEquals(expectMemSize, testingMm.getMemoryUsedInBytes());
    }
}