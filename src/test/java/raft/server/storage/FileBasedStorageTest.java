package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/6/12
 */
public class FileBasedStorageTest {
    private static final String testingDirectory = "./target/storage";
    private static final String storageName = "testing_storage";

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory, storageName));
    }

    @Test
    public void appendToMm() throws Exception {
        FileBasedStorage storage = new FileBasedStorage(testingDirectory, storageName);
        storage.init();

        List<LogEntry> expectEntries = TestUtil.newLogEntryList(100, 10240, 40960);

        List<List<LogEntry>> batches = TestUtil.randomPartitionLogEntryList(expectEntries);
        for (List<LogEntry> batch : batches) {
            storage.append(batch);
        }

        assertEquals(expectEntries.get(0).getIndex(), storage.getFirstIndex());
        assertEquals(expectEntries.get(expectEntries.size() - 1).getIndex(), storage.getLastIndex());

        List<LogEntry> actualEntries = storage.getEntries(storage.getFirstIndex(), storage.getLastIndex() + 1);
        for (int i = 0; i < expectEntries.size(); i++) {
            LogEntry expectEntry = expectEntries.get(i);
            LogEntry actualEntry = actualEntries.get(i);

            assertEquals(expectEntry.getIndex(), actualEntry.getIndex());
            assertEquals(expectEntry.getTerm(), actualEntry.getTerm());
            assertEquals(expectEntry.getData(), actualEntry.getData());
        }
    }

    @Test
    public void recoverMmFromLog() throws Exception {
        FileBasedStorage storage = new FileBasedStorage(testingDirectory, storageName);
        storage.init();

        List<LogEntry> expectEntries = TestUtil.newLogEntryList(100, 10240, 40960);

        List<List<LogEntry>> batches = TestUtil.randomPartitionLogEntryList(expectEntries);
        for (List<LogEntry> batch : batches) {
            storage.append(batch);
        }
        storage.destroyStorage();

        storage = new FileBasedStorage(testingDirectory, storageName);
        storage.init();

        assertEquals(expectEntries.get(0).getIndex(), storage.getFirstIndex());
        assertEquals(expectEntries.get(expectEntries.size() - 1).getIndex(), storage.getLastIndex());

        List<LogEntry> actualEntries = storage.getEntries(storage.getFirstIndex(), storage.getLastIndex() + 1);
        for (int i = 0; i < expectEntries.size(); i++) {
            LogEntry expectEntry = expectEntries.get(i);
            LogEntry actualEntry = actualEntries.get(i);

            assertEquals(expectEntry.getIndex(), actualEntry.getIndex());
            assertEquals(expectEntry.getTerm(), actualEntry.getTerm());
            assertEquals(expectEntry.getData(), actualEntry.getData());
        }
    }

    @Test
    public void appendAndReadEntries() throws Exception {
        FileBasedStorage storage = new FileBasedStorage(testingDirectory, storageName);
        storage.init();

        int dataLowSize = 1024;
        int expectLogCount = Constant.kMaxMemtableSize / dataLowSize;
        // create at least 2 tables
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(expectLogCount * 2, dataLowSize, 2048);
        int firstIndex = expectEntries.get(0).getIndex();
        int lastIndex = expectEntries.get(expectEntries.size() - 1).getIndex();

        List<List<LogEntry>> batches = TestUtil.randomPartitionLogEntryList(expectEntries);
        for (List<LogEntry> batch : batches) {
            storage.append(batch);
            assertEquals(firstIndex, storage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), storage.getLastIndex());
        }

        int cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = storage.getEntries(cursor, cursor + step);
            List<LogEntry> expect = expectEntries.subList(cursor - firstIndex, Math.min(cursor + step - firstIndex, expectEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                LogEntry expectEntry = expect.get(i);
                assertEquals(expectEntry, actual.get(i));
                assertEquals(expectEntry.getTerm(), storage.getTerm(expectEntry.getIndex()));
            }

            cursor += step;
        }
    }
}