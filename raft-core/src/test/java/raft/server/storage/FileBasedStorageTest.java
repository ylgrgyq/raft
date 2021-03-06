package raft.server.storage;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;
import raft.server.proto.LogEntry;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/6/12
 */
public class FileBasedStorageTest {
    private static final String testingDirectory = "./target/storage";
    private static final String storageName = "TestingStorage";
    private static FileBasedStorage testingStorage;

    @Before
    public void setUp() {
        TestUtil.cleanDirectory(Paths.get(testingDirectory, storageName));
        testingStorage = new FileBasedStorage(testingDirectory, storageName);
        testingStorage.init();
    }

    @After
    public void tearDown() throws Exception {
        testingStorage.awaitTermination();
    }

    @Test
    public void recover() throws Exception {
        int dataLowSize = 1024;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(10, dataLowSize, dataLowSize + 1);
        for (List<LogEntry> batch : TestUtil.randomPartitionList(expectEntries)) {
            testingStorage.append(batch);
        }

        checkAllEntries(expectEntries);

        reOpen();

        checkAllEntries(expectEntries);
        LogEntry lastE = expectEntries.get(expectEntries.size() - 1);
        List<LogEntry> newEntries = TestUtil.newLogEntryList(10, dataLowSize,
                dataLowSize + 1, lastE.getTerm(), lastE.getIndex());
        for (List<LogEntry> batch : TestUtil.randomPartitionList(newEntries)) {
            testingStorage.append(batch);
        }
        expectEntries.addAll(newEntries);
        checkAllEntries(expectEntries);

        reOpen();

        checkAllEntries(expectEntries);
    }

    @Test
    public void recoverThenUseNewLogFile() throws Exception {
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

        reOpen();

        checkAllEntries(expectEntries);
    }

    @Test
    public void recoverDuringCompaction() throws Exception {
        int dataLowSize = 1024;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(10, dataLowSize, dataLowSize + 1);
        testingStorage.append(expectEntries);

        // trigger compaction
        LogEntry lastE = expectEntries.get(expectEntries.size() - 1);
        List<LogEntry> newEntries = TestUtil.newLogEntryList(2,
                2 * Constant.kMaxMemtableSize,
                2* Constant.kMaxMemtableSize + 1,
                lastE.getTerm(),
                lastE.getIndex()
        );

        expectEntries.addAll(newEntries);
        testingStorage.append(newEntries);
        testingStorage.awaitTermination();

        reOpen();

        checkAllEntries(expectEntries);
    }

    private void reOpen() throws IOException, InterruptedException {
        testingStorage.awaitTermination();

        testingStorage = new FileBasedStorage(testingDirectory, storageName);
        testingStorage.init();
    }

    private void checkAllEntries(List<LogEntry> expectEntries) {
        assertEquals(expectEntries.get(0).getIndex(), testingStorage.getFirstIndex());
        assertEquals(expectEntries.toString(), expectEntries.get(expectEntries.size() - 1).getIndex(), testingStorage.getLastIndex());

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
        long firstIndex = expectEntries.get(0).getIndex();
        long lastIndex = expectEntries.get(expectEntries.size() - 1).getIndex();

        List<List<LogEntry>> batches = TestUtil.randomPartitionList(expectEntries);
        for (List<LogEntry> batch : batches) {
            testingStorage.append(batch);
            assertEquals(firstIndex, testingStorage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), testingStorage.getLastIndex());
        }

        long cursor = firstIndex;
        while (cursor < lastIndex + 1) {
            int step = ThreadLocalRandom.current().nextInt(10, 1000);
            List<LogEntry> actual = testingStorage.getEntries(cursor, cursor + step);
            List<LogEntry> expect = expectEntries.subList((int)(cursor - firstIndex), Math.min((int)(cursor + step - firstIndex), expectEntries.size()));
            for (int i = 0; i < expect.size(); i++) {
                LogEntry expectEntry = expect.get(i);
                assertEquals(expectEntry, actual.get(i));
                assertEquals(expectEntry.getTerm(), testingStorage.getTerm(expectEntry.getIndex()));
            }

            cursor += step;
        }
    }

    private Set<Integer> getSstableFileNumbers() {
        Path dirPath = Paths.get(testingDirectory);
        assert Files.isDirectory(dirPath);
        File dirFile = new File(testingDirectory + "/" + storageName);
        File[] files = dirFile.listFiles();

        if (files != null) {
            return Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .map(FileName::parseFileName)
                    .filter(meta -> meta.getType() == FileName.FileType.SSTable)
                    .map(FileName.FileNameMeta::getFileNumber)
                    .collect(Collectors.toSet());
        } else {
            return Collections.emptySet();
        }
    }

    private LogEntry triggerFlushLogToSSTable(LogEntry lastE) throws InterruptedException {
        List<LogEntry> newEntries = TestUtil.newLogEntryList(1,
                Constant.kMaxMemtableSize + 1,
                Constant.kMaxMemtableSize + 2,
                lastE.getTerm(),
                lastE.getIndex()
        );

        testingStorage.append(newEntries);
        testingStorage.waitWriteSstableFinish();
        return newEntries.get(0);
    }

    @Test
    public void overWriteEntries() throws Exception {
        int dataSize = 1024;
        int expectSstableCount = 3;
        int entryPerTable = Constant.kMaxMemtableSize / dataSize;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(entryPerTable * expectSstableCount, dataSize);
        long firstIndex = expectEntries.get(0).getIndex();

        List<List<LogEntry>> batches = TestUtil.randomPartitionList(expectEntries);
        for (List<LogEntry> batch : batches) {
            testingStorage.append(batch);
            assertEquals(firstIndex, testingStorage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), testingStorage.getLastIndex());
        }

        testingStorage.waitWriteSstableFinish();
        Set<Integer> sstableFileNumbers = getSstableFileNumbers();
        assertEquals(expectSstableCount, sstableFileNumbers.size());

        // overwrite entries in the middle of the second table
        long overwriteStartIndex = firstIndex  + entryPerTable + entryPerTable / 2;
        long overwriteStartTerm = expectEntries.get((int)(overwriteStartIndex - firstIndex)).getTerm();
        List<LogEntry> overwriteEntries = TestUtil.newLogEntryList(entryPerTable, dataSize, dataSize + 1,
                overwriteStartTerm, overwriteStartIndex, 1, 1);

        batches = TestUtil.randomPartitionList(overwriteEntries);
        for (List<LogEntry> batch : batches) {
            testingStorage.append(batch);
            assertEquals(firstIndex, testingStorage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), testingStorage.getLastIndex());
        }

        // trigger flush
         triggerFlushLogToSSTable(overwriteEntries.get(overwriteEntries.size() - 1));
        testingStorage.waitWriteSstableFinish();
        sstableFileNumbers = getSstableFileNumbers();
        assertEquals(2, sstableFileNumbers.size());

        // check old entries
        long cursor = firstIndex;
        while (cursor < overwriteStartIndex) {
            List<LogEntry> actual = testingStorage.getEntries(cursor, cursor + 1);
            LogEntry expect = expectEntries.get((int)(cursor - firstIndex));
            assertEquals(expect, actual.get(0));
            assertEquals(expect.getTerm(), testingStorage.getTerm(expect.getIndex()));

            ++cursor;
        }

        // check overwritten entries
        while (cursor < overwriteStartIndex + overwriteEntries.size()) {
            List<LogEntry> actual = testingStorage.getEntries(cursor, cursor + 1);
            LogEntry expect = overwriteEntries.get((int)(cursor - overwriteStartIndex));
            assertEquals(expect, actual.get(0));
            assertEquals(expect.getTerm(), testingStorage.getTerm(expect.getIndex()));

            ++cursor;
        }
    }

    @Test
    public void compact() throws Exception {
        int dataSize = 1024;
        int expectSstableCount = 3;
        int entryPerTable = Constant.kMaxMemtableSize / dataSize;
        List<LogEntry> expectEntries = TestUtil.newLogEntryList(entryPerTable * expectSstableCount, dataSize);
        long firstIndex = expectEntries.get(0).getIndex();
        long lastIndex = expectEntries.get(expectEntries.size() - 1).getIndex();

        List<List<LogEntry>> batches = TestUtil.randomPartitionList(expectEntries);
        for (List<LogEntry> batch : batches) {
            testingStorage.append(batch);
            assertEquals(firstIndex, testingStorage.getFirstIndex());
            assertEquals(batch.get(batch.size() - 1).getIndex(), testingStorage.getLastIndex());
        }

        // trigger flush
        LogEntry lastE = expectEntries.get(expectEntries.size() - 1);
        lastE = triggerFlushLogToSSTable(lastE);
        testingStorage.waitWriteSstableFinish();

        Set<Integer> sstableFileNumbers = getSstableFileNumbers();
        assertEquals(expectSstableCount, sstableFileNumbers.size());

        // start compact at the middle of the second table
        long compactCursor = firstIndex  + entryPerTable / 2;
        while (compactCursor < lastIndex + 1) {
            Future<Long> future = testingStorage.compact(compactCursor);
            lastE = triggerFlushLogToSSTable(lastE);
            testingStorage.waitWriteSstableFinish();
            long actualToIndex = future.get();
            assertEquals(actualToIndex, testingStorage.getFirstIndex());
            assertTrue(actualToIndex <= compactCursor);
            sstableFileNumbers = getSstableFileNumbers();
            assertEquals(expectSstableCount + 1, sstableFileNumbers.size());

            long cursor = compactCursor;
            while (cursor < lastIndex + 1) {
                int step = ThreadLocalRandom.current().nextInt(10, 1000);
                List<LogEntry> actual = testingStorage.getEntries(cursor, cursor + step);
                List<LogEntry> expect = expectEntries.subList((int)(cursor - firstIndex), Math.min((int)(cursor + step - firstIndex), expectEntries.size()));
                for (int i = 0; i < expect.size(); i++) {
                    LogEntry expectEntry = expect.get(i);
                    assertEquals(expectEntry, actual.get(i));
                    assertEquals(expectEntry.getTerm(), testingStorage.getTerm(expectEntry.getIndex()));
                }

                cursor += step;
            }
            compactCursor += entryPerTable;
        }
    }
}