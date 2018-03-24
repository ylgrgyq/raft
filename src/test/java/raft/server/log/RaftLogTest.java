package raft.server.log;

import org.junit.Test;
import raft.server.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/3/22
 */
public class RaftLogTest {
    private static int initTerm = 12345;
    private static byte[] data = new byte[]{0, 1, 2, 3, 4};

    private static LogEntry newLogEntry(int index, int term) {
        LogEntry e = new LogEntry();
        e.setTerm(term);
        e.setData(data);
        e.setIndex(index);
        return e;
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

        // append some logs
        int logsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(logsCount);
        log.append(entries);
        assertEquals(logsCount, log.getLastIndex());

        // check appended logs
        for (int i = 1; i < logsCount + 1; i++) {
            LogEntry e = log.getEntry(i).get();
            assertEquals(i, e.getIndex());
            assertEquals(entries.get(i - 1), e);
        }

        List<LogEntry> currentEntries = log.getEntries(1, logsCount + 1);
        for (int i = 1; i < logsCount + 1; i++) {
            assertEquals(entries.get(i - 1), currentEntries.get(i - 1));
        }
    }

    @Test
    public void testTruncate() throws Exception {
        RaftLog log = new RaftLog();

        // append some logs
        int logsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(logsCount);
        log.append(entries);
        assertEquals(logsCount, log.getLastIndex());
        log.tryCommitTo(logsCount);

        log.truncate(logsCount);

        // append more logs
        int moreLogsCount = ThreadLocalRandom.current().nextInt(1, 100);
        List<LogEntry> moreEntries = RaftLogTest.newLogEntryList(moreLogsCount);
        log.append(moreEntries);
        assertEquals(logsCount + moreLogsCount, log.getLastIndex());

        // check appended logs
        for (int i = logsCount + 1; i < logsCount + moreLogsCount; i++) {
            LogEntry e = log.getEntry(i).get();
            assertEquals(i, e.getIndex());
            assertEquals(moreEntries.get(i - logsCount - 1), e);
        }

        List<LogEntry> currentEntries = log.getEntries(logsCount, logsCount + moreLogsCount + 1);
        assertEquals(entries.get(entries.size() - 1), currentEntries.get(0));
        for (int i = 0; i < moreLogsCount; i++) {
            assertEquals(moreEntries.get(i), currentEntries.get(i + 1));
        }
    }

    private RaftLog createRaftLogWithSomeInitLogs(int logsCount, int commitTo) {
        RaftLog log = new RaftLog();

        // append some logs
        List<LogEntry> entries = RaftLogTest.newLogEntryList(logsCount);
        log.append(entries);
        assertEquals(logsCount, log.getLastIndex());

        log.tryCommitTo(commitTo);
        assertEquals(commitTo, log.getCommitIndex());

        return log;
    }

    private void assertLogEntriesEquals(List<LogEntry> expect, List<LogEntry> actual) {
        assertEquals(expect.size(), actual.size());
        for (int i = 0; i < expect.size(); ++i) {
            assertEquals(expect.get(i), actual.get(i));
        }
    }

    @Test
    public void testTryAppendEntriesWithoutConflict() throws Exception {
        RaftLog log;

        // testing prevIndex <= entriesLastIndex <= commitIndex <= lastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, 20);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));

        // testing prevIndex <= commitIndex <= entriesLastIndex <= lastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, 55);
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(prevIndex, log.getLastIndex() + 1));

        // testing commitIndex <= prevIndex <= entriesLastIndex <= lastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(50, 75);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, 20);
        logsUnchanged = log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1));

        // testing prevIndex <= commitIndex <= lastIndex <= entriesLastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, 100);
        int prevLastIndex = log.getLastIndex();
        logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertLogEntriesEquals(logsUnchanged, log.getEntries(prevIndex, prevLastIndex + 1));
        assertLogEntriesEquals(entries, log.getEntries(prevIndex + 1, log.getLastIndex() + 1));


        // testing commitIndex <= prevIndex <= lastIndex <= entriesLastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(50, 75);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, 100);
        prevLastIndex = log.getLastIndex();
        int prevCommitIndex = log.getCommitIndex();
        logsUnchanged = log.getEntries(log.getCommitIndex(), log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        List<LogEntry> es = log.getEntries(prevCommitIndex, prevLastIndex + 1);
        assertLogEntriesEquals(logsUnchanged, es);
        es = log.getEntries(prevIndex + 1, log.getLastIndex() + 1);
        assertLogEntriesEquals(entries, es);

        // testing commitIndex <= lastIndex <= prevIndex <= entriesLastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(100, 200);
        entries = RaftLogTest.newLogEntryList(20);
        assertTrue(! log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
    }

    @Test
    public void testTryAppendEntriesWithConflict() throws Exception {
        RaftLog log;

        // testing prevIndex <= conflictIndex <= commitIndex <= lastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        int prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        int conflictIndex = ThreadLocalRandom.current().nextInt(prevIndex + 1, 49);
        List<LogEntry> entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex,100);
        try {
            log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries);
            assertTrue(false);
        } catch (RuntimeException ex) {
            // ignore
        }

        // testing commitIndex <= conflictIndex <= lastIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        conflictIndex = ThreadLocalRandom.current().nextInt(51, 100);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex, 100);
        List<LogEntry> logsUnchanged = log.getEntries(prevIndex, conflictIndex);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertEquals(logsUnchanged, log.getEntries(prevIndex, conflictIndex));
        List<LogEntry> e1 = entries.subList(conflictIndex - prevIndex - 1, entries.size());
        List<LogEntry> e2 = log.getEntries(conflictIndex, prevIndex + entries.size() + 1);
        assertEquals(e1, e2);

        // testing prevIndex <= commitIndex <= lastIndex <= conflictIndex
        log = this.createRaftLogWithSomeInitLogs(100, 50);
        prevIndex = ThreadLocalRandom.current().nextInt(0, 30);
        conflictIndex = ThreadLocalRandom.current().nextInt(100, 200);
        entries = RaftLogTest.newLogEntryList(prevIndex + 1, conflictIndex,300);
        int prevLastIndex = log.getLastIndex();
        logsUnchanged = log.getEntries(prevIndex, log.getLastIndex() + 1);
        assertTrue(log.tryAppendEntries(prevIndex, initTerm, prevIndex + entries.size(), entries));
        assertLogEntriesEquals(logsUnchanged, log.getEntries(prevIndex, prevLastIndex + 1));
        assertLogEntriesEquals(entries, log.getEntries(prevIndex + 1, log.getLastIndex() + 1));
    }
}