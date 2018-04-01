package raft.server.log;

import com.google.protobuf.ByteString;
import org.junit.Test;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/3/22
 */
public class RaftLogTest {
    private static int initTerm = 12345;
    private static byte[] data = new byte[]{0, 1, 2, 3, 4};

    private static List<byte[]> newDataList(int count) {
        List<byte[]> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            dataList.add(data);
        }
        return dataList;
    }

    private static LogEntry newLogEntry(int index, int term) {
        return LogEntry.newBuilder()
                .setData(ByteString.copyFrom(data))
                .setTerm(term)
                .setIndex(index)
                .build();
    }

    private static List<LogEntry> newLogEntryList(int count) {
        return RaftLogTest.newLogEntryList(0, count);
    }

    private static List<LogEntry> newLogEntryList(int fromIndex, int count) {
        return RaftLogTest.newLogEntryList(fromIndex, Integer.MAX_VALUE, count);
    }

    private static List<LogEntry> newLogEntryList(int fromIndex, int conflictIndex, int count) {
        ArrayList<LogEntry> entries = new ArrayList<>();
        for (int i = fromIndex; i < fromIndex + count; i++) {
            int term = initTerm;
            if (i >= conflictIndex) {
                ++term;
            }
            entries.add(RaftLogTest.newLogEntry(i, term));
        }
        return entries;
    }

    @Test
    public void testRaftLogInitState() throws Exception {
        RaftLog log = new RaftLog();

        // check RaftLog init state
        assertEquals(0, log.getLastIndex());
        assertEquals(0, (long) log.getTerm(0).get());
        assertEquals(-1, log.getCommitIndex());
        assertEquals(RaftLog.sentinel, log.getEntry(0).get());
        List<LogEntry> currentEntries = log.getEntries(0, 100);
        assertEquals(1, currentEntries.size());
        assertEquals(RaftLog.sentinel, currentEntries.get(0));
    }

    @Test
    public void testPlainAppend() throws Exception {
        RaftLog log = new RaftLog();

        // directAppend some logs
        int logsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<byte[]> entries = RaftLogTest.newDataList(logsCount);
        log.directAppend(initTerm, entries);
        assertEquals(logsCount, log.getLastIndex());

        // check appended logs
        for (int i = 1; i < logsCount + 1; i++) {
            LogEntry e = log.getEntry(i).get();
            assertEquals(i, e.getIndex());
            assertArrayEquals(entries.get(i - 1), e.getData().toByteArray());
        }

        List<LogEntry> currentEntries = log.getEntries(1, logsCount + 1);
        for (int i = 1; i < logsCount + 1; i++) {
            assertArrayEquals(entries.get(i - 1), currentEntries.get(i - 1).getData().toByteArray());
        }
    }

    @Test
    public void testTruncate() throws Exception {
        RaftLog log = new RaftLog();

        // directAppend some logs
        int logsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<byte[]> entries = RaftLogTest.newDataList(logsCount);
        log.directAppend(initTerm, entries);
        assertEquals(logsCount, log.getLastIndex());
        log.tryCommitTo(logsCount);

        log.truncate(logsCount);

        // directAppend more logs
        int moreLogsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<byte[]> moreEntries = RaftLogTest.newDataList(moreLogsCount);
        log.directAppend(initTerm, moreEntries);
        assertEquals(logsCount + moreLogsCount, log.getLastIndex());

        // check appended logs
        for (int i = logsCount + 1; i < logsCount + moreLogsCount; i++) {
            LogEntry e = log.getEntry(i).get();
            assertEquals(i, e.getIndex());
            assertArrayEquals(moreEntries.get(i - logsCount - 1), e.getData().toByteArray());
        }

        List<LogEntry> currentEntries = log.getEntries(logsCount, logsCount + moreLogsCount + 1);
        assertArrayEquals(entries.get(entries.size() - 1), currentEntries.get(0).getData().toByteArray());
        for (int i = 0; i < moreLogsCount; i++) {
            assertArrayEquals(moreEntries.get(i), currentEntries.get(i + 1).getData().toByteArray());
        }
    }

    private RaftLog createRaftLogWithSomeInitLogs(int logsCount, int commitTo) {
        RaftLog log = new RaftLog();

        // append some logs
        List<byte[]> entries = RaftLogTest.newDataList(logsCount);
        log.directAppend(initTerm, entries);
        assertEquals(logsCount, log.getLastIndex());

        log.tryCommitTo(commitTo);
        assertEquals(commitTo, log.getCommitIndex());

        return log;
    }

    private void assertLogEntriesEquals(List<LogEntry> expect, List<LogEntry> actual) {
        assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); ++i) {
            LogEntry e = expect.get(i);
            LogEntry a = actual.get(i);
            assertEquals(e.getTerm(), a.getTerm());
            assertEquals(e.getIndex(), a.getIndex());
            assertArrayEquals(e.getData().toByteArray(), a.getData().toByteArray());
        }
    }

    /**
     * testing prevIndex <= entriesLastIndex <= commitIndex <= lastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict0() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 20);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
    }

    /**
     * testing prevIndex <= commitIndex <= entriesLastIndex <= lastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict1() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 55);
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(prevIndex, log.getLastIndex() + 1));
    }

    /**
     * testing commitIndex <= prevIndex <= entriesLastIndex <= lastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict2() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(50, 75);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 20);
        List<LogEntry> logsUnchanged = log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1));
    }

    /**
     * testing prevIndex <= commitIndex <= lastIndex <= entriesLastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict3() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 100);
        int prevLastIndex = log.getLastIndex();
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertLogEntriesEquals(logsUnchanged, log.getEntries(prevIndex, prevLastIndex + 1));
        assertLogEntriesEquals(entries, log.getEntries(prevIndex + 1, log.getLastIndex() + 1));
    }

    /**
     * testing commitIndex <= prevIndex <= lastIndex <= entriesLastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict4() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(50, 75);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 100);
        int prevLastIndex = log.getLastIndex();
        int prevCommitIndex = log.getCommitIndex();
        List<LogEntry> logsUnchanged = log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        List<LogEntry> es = log.getEntries(prevCommitIndex, prevLastIndex + 1);
        assertLogEntriesEquals(logsUnchanged, es);
        es = log.getEntries(prevIndex + 1, log.getLastIndex() + 1);
        assertLogEntriesEquals(entries, es);
    }

    /**
     * testing commitIndex <= lastIndex <= prevIndex <= entriesLastIndex
     */
    @Test
    public void testTryAppendEntriesWithoutConflict5() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(100, 200);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(20);
        assertTrue(! log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
    }

    /**
     * testing prevIndex <= conflictIndex <= commitIndex <= lastIndex
     */
    @Test(expected = RuntimeException.class)
    public void testTryAppendEntriesWithConflict0() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        int conflictIndex = ThreadLocalRandom.current().nextInt(prevIndex + 1, 49);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex,100);
        log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries);
    }

    /**
     * testing commitIndex <= conflictIndex <= lastIndex
     */
    @Test
    public void testTryAppendEntriesWithConflict1() throws Exception {
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        int conflictIndex = ThreadLocalRandom.current().nextInt(51, 100);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex, 100);
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, conflictIndex);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(prevIndex, conflictIndex));
        List<LogEntry> e1 = entries.subList(conflictIndex - prevIndex - 1, entries.size());
        List<LogEntry> e2 = log.getEntries(conflictIndex, prevIndex + entries.size() + 1);
        assertEquals(e1, e2);
    }

    /**
     * testing prevIndex <= commitIndex <= lastIndex <= conflictIndex
     */
    @Test
    public void testTryAppendEntriesWithConflict2() throws Exception {
        //
        RaftLog log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        int conflictIndex = ThreadLocalRandom.current().nextInt(100, 200);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex,300);
        int prevLastIndex = log.getLastIndex();
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertLogEntriesEquals(logsUnchanged, log.getEntries(prevIndex, prevLastIndex + 1));
        assertLogEntriesEquals(entries, log.getEntries(prevIndex + 1, log.getLastIndex() + 1));
    }
}