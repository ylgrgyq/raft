package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Author: ylgrgyq
 * Date: 18/7/2
 */
public class ManifestTest {
    private static final String testingDirectory = "./target/storage";
    private static final String storageName = "manifest_test";

    private Manifest testingManifest;

    @Before
    public void setUp() {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));
    }

    private List<SSTableFileMetaInfo> generateFixedIntervalMetas(int count, int startKey, int interval) {
        List<SSTableFileMetaInfo> ret = new ArrayList<>(count);

        int k = startKey;
        for (int i = 1; i <= count; i++) {
            SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setFileSize(ThreadLocalRandom.current().nextInt());
            meta.setFileNumber(i);
            meta.setFirstKey(k);
            k += interval;
            meta.setLastKey(k);
            ret.add(meta);
            k += interval;
        }

        return ret;
    }

    @Test
    public void recovery() throws Exception {
        testingManifest = new Manifest(testingDirectory, storageName);
        int count = ThreadLocalRandom.current().nextInt(100, 10000);
        List<SSTableFileMetaInfo> expectMetas = generateFixedIntervalMetas(count,
                ThreadLocalRandom.current().nextInt(100, 10000),
                ThreadLocalRandom.current().nextInt(1, 100));
        ManifestRecord lastRecord = logManifestRecord(expectMetas);
        assertNotNull(lastRecord);
        int expectLogNumber = lastRecord.getLogNumber();
        int expectNextFileNumber = lastRecord.getNextFileNumber();

        testingManifest = new Manifest(testingDirectory, storageName);
        String current = FileName.getCurrentManifestFileName(storageName);
        String manifestFileName = new String(Files.readAllBytes(Paths.get(testingDirectory, current)), StandardCharsets.UTF_8);
        testingManifest.recover(manifestFileName);

        assertEquals(expectNextFileNumber, testingManifest.getNextFileNumber());
        assertEquals(expectLogNumber, testingManifest.getLogFileNumber());

        int firstIndex = testingManifest.getFirstIndex();
        int lastIndex = testingManifest.getLastIndex();
        assertEquals(firstIndex, expectMetas.get(0).getFirstKey());
        assertEquals(lastIndex, expectMetas.get(expectMetas.size() - 1).getLastKey());

        List<SSTableFileMetaInfo> metas = testingManifest.searchMetas(firstIndex, lastIndex + 1);
        assertEquals(expectMetas, metas);
    }

    private ManifestRecord logManifestRecord(List<SSTableFileMetaInfo> expectMetas) throws IOException {
        ManifestRecord record = null;
        int logNumber = 1;
        int i = 0;
        while (i < expectMetas.size()) {
            int count = ThreadLocalRandom.current().nextInt(1, 10);
            List<SSTableFileMetaInfo> batch = expectMetas.subList(i, Math.min(i + count, expectMetas.size()));
            record = new ManifestRecord();
            record.setLogNumber(logNumber);
            record.addMetas(batch);
            if (ThreadLocalRandom.current().nextDouble() > 0.5) {
                ++logNumber;
            }
            if (ThreadLocalRandom.current().nextDouble() > 0.5) {
                testingManifest.getNextFileNumber();
            }
            testingManifest.logRecord(record);
            i += count;
        }
        return record;
    }

    @Test
    public void binarySearchMetas() throws Exception {
        testingManifest = new Manifest(testingDirectory, storageName);
        // origin metas' key range is 100 ~ 200, 300 ~ 400, 500 ~ 600, ...., 19900 ~ 20000
        List<SSTableFileMetaInfo> metas = generateFixedIntervalMetas(100, 100, 100);
        ManifestRecord record = new ManifestRecord();
        record.addMetas(metas);
        testingManifest.logRecord(record);
        searchMetas0(metas);
    }

    @Test
    public void traverseSearchMetas() throws Exception {
        testingManifest = new Manifest(testingDirectory, storageName);
        // origin metas' key range is 100 ~ 200, 300 ~ 400, 500 ~ 600, ...., 3100 ~ 3200
        List<SSTableFileMetaInfo> metas = generateFixedIntervalMetas(16, 100, 100);
        ManifestRecord record = new ManifestRecord();
        record.addMetas(metas);
        testingManifest.logRecord(record);
        searchMetas0(metas);
    }

    private void searchMetas0(List<SSTableFileMetaInfo> metas){
        int start = 0;
        int round = 0;
        while (start < metas.size()) {
            int end = Math.min(start + 3, metas.size());
            int startKey = start * 200 + 100;
            int endKey = end * 200;

            List<SSTableFileMetaInfo> expectMetas = metas.subList(start, end);
            int index = 0;
            List<SSTableFileMetaInfo> retMetas = testingManifest.searchMetas(startKey - 20, endKey + 60);
            for (SSTableFileMetaInfo meta : retMetas) {
                assertEquals(expectMetas.get(index++), meta);
            }
            assertEquals(expectMetas.size(), index);

            index = 0;
            retMetas = testingManifest.searchMetas(startKey, endKey + 60);
            for (SSTableFileMetaInfo meta : retMetas) {
                assertEquals(expectMetas.get(index++), meta);
            }
            assertEquals(expectMetas.size(), index);

            index = 0;
            retMetas = testingManifest.searchMetas(startKey + 10, endKey);
            for (SSTableFileMetaInfo meta : retMetas) {
                assertEquals(expectMetas.get(index++), meta);
            }
            assertEquals(expectMetas.size(), index);

            index = 0;
            retMetas = testingManifest.searchMetas(startKey + 100, endKey);
            for (SSTableFileMetaInfo meta : retMetas) {
                assertEquals(expectMetas.get(index++), meta);
            }
            assertEquals(expectMetas.size(), index);

            start = ++round * 3;
        }
    }
}