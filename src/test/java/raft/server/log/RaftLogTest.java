package raft.server.log;

import org.junit.Test;
import raft.server.LogEntry;
import sun.rmi.runtime.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/3/22
 */
public class RaftLogTest {
    private static int term = 12345;
    private static byte[] data = new byte[]{0, 1, 2, 3, 4};

    private static LogEntry newLogEntry() {
        LogEntry e = new LogEntry();
        e.setTerm(term);
        e.setData(data);
        return e;
    }

    private static List<LogEntry> newLogEntryList(int count) {
        ArrayList<LogEntry> entries = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            entries.add(RaftLogTest.newLogEntry());
        }
        return entries;
    }

    @Test
    public void testRaftLogInitState() throws Exception {
        RaftLog log = new RaftLog();

        // check RaftLog init state
        assertEquals(0, log.getLastIndex());
        assertEquals(0, (long)log.getTerm(0).get());
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

        log.truncate();

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

    @Test
    public void testTryAppendEntries() throws Exception {

    }
}