package raft.server.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/6/12
 */
public class FileBasedStorageTest {
    private static final String testingDirectory = "./target/storage";
    private static final String storageName = "TestingStorage";
    private static FileBasedStorage testingStorage;

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory, storageName));
        testingStorage = new FileBasedStorage(testingDirectory, storageName);
        testingStorage.init();
    }

    @After
    public void tearDown() throws Exception {
        testingStorage.awaitShutdown(5, TimeUnit.SECONDS);
    }

    @Test
    public void recover() throws Exception {
        int dataLowSize = 1024;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(10, dataLowSize, dataLowSize + 1);
        for (List<LogEntry> batch : TestUtil.randomPartitionLogEntryList(expectEntries)) {
            testingStorage.append(batch);
        }

        checkAllEntries(expectEntries);

        reOpen();

        checkAllEntries(expectEntries);
        List<LogEntry> newEntries = TestUtil.newLogEntryList(10, dataLowSize, dataLowSize + 1);
        for (List<LogEntry> batch : TestUtil.randomPartitionLogEntryList(newEntries)) {
            testingStorage.append(batch);
        }
        expectEntries.addAll(newEntries);
        checkAllEntries(expectEntries);

        reOpen();

        checkAllEntries(expectEntries);
    }

    @Test
    public void recoverFromEmptyLogFile() throws Exception {
        int dataLowSize = 1024;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(10, dataLowSize, dataLowSize + 1);
        testingStorage.append(expectEntries);

        // trigger compaction
        LogEntry lastE = expectEntries.get(expectEntries.size() - 1);
        List<LogEntry> newEntries = TestUtil.newLogEntryList(2,
                Constant.kMaxMemtableSize,
                Constant.kMaxMemtableSize + 1,
                lastE.getTerm(),
                lastE.getIndex()
        );

        expectEntries.addAll(newEntries);
        testingStorage.append(newEntries);

        checkAllEntries(expectEntries);
        testingStorage.waitWriteSstableFinish();

        reOpen();

        checkAllEntries(expectEntries);
    }

    private void reOpen() throws IOException {
        testingStorage.awaitShutdown(5, TimeUnit.SECONDS);

        testingStorage = new FileBasedStorage(testingDirectory, storageName);
        testingStorage.init();
    }

    private void checkAllEntries(List<LogEntry> expectEntries) {
        assertEquals(expectEntries.get(0).getIndex(), testingStorage.getFirstIndex());
        assertEquals(expectEntries.get(expectEntries.size() - 1).getIndex(), testingStorage.getLastIndex());

        List<LogEntry> actualEntries = testingStorage.getEntries(testingStorage.getFirstIndex(), testingStorage.getLastIndex() + 1);
        for (int i = 0; i < expectEntries.size(); i++) {
            assertEquals(expectEntries.get(i), actualEntries.get(i));
        }
    }

    @Test
    public void appendAndReadEntries() throws Exception {
        int dataLowSize = 1024;
        int expectLogCount = Constant.kMaxMemtableSize / dataLowSize;
        // create at least 2 tables
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(expectLogCount * 2, dataLowSize, 2048);
        int firstIndex = expectEntries.get(0).getIndex();
        int lastIndex = expectEntries.get(expectEntries.size() - 1).getIndex();

        List<List<LogEntry>> batches = TestUtil.randomPartitionLogEntryList(expectEntries);
        for (List<LogEntry> batch : batches) {
            testingStorage.append(batch);
            assertEquals(firstIndex, testingStorage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), testingStorage.getLastIndex());
        }

        int cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = testingStorage.getEntries(cursor, cursor + step);
            List<LogEntry> expect = expectEntries.subList(cursor - firstIndex, Math.min(cursor + step - firstIndex, expectEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                LogEntry expectEntry = expect.get(i);
                assertEquals(expectEntry, actual.get(i));
                assertEquals(expectEntry.getTerm(), testingStorage.getTerm(expectEntry.getIndex()));
            }

            cursor += step;
        }
    }
}