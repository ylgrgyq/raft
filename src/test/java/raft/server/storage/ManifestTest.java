package raft.server.storage;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/7/2
 */
public class ManifestTest {
    private static final String testingDirectory = "./target/storage";
    private static final String storageName = "manifest_test";

    private Manifest testingManifest;

    @Before
    public void setUp() throws Exception {

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
    public void searchMetas() throws Exception {
        // binary search metas
        testingManifest = new Manifest(testingDirectory, storageName);
        // origin metas' key range is 100 ~ 200, 300 ~ 400, 500 ~ 600, ...., 19900 ~ 20000
        List<SSTableFileMetaInfo> metas = generateFixedIntervalMetas(100, 100, 100);
        for (SSTableFileMetaInfo meta : metas) {
            testingManifest.registerMeta(meta);
        }
        searchMetas0(metas);

        // traverse search metas
        testingManifest = new Manifest(testingDirectory, storageName);
        // origin metas' key range is 100 ~ 200, 300 ~ 400, 500 ~ 600, ...., 3100 ~ 3200
        metas = generateFixedIntervalMetas(16, 100, 100);
        for (SSTableFileMetaInfo meta : metas) {
            testingManifest.registerMeta(meta);
        }
        searchMetas0(metas);
    }

    private void searchMetas0(List<SSTableFileMetaInfo> metas){
        // get first three metas
        List<SSTableFileMetaInfo> expectMetas = metas.subList(0, 3);
        int index = 0;
        Iterator<SSTableFileMetaInfo> itr = testingManifest.searchMetas(80, 660);
        while (itr.hasNext()) {
            SSTableFileMetaInfo meta = itr.next();
            assertEquals(expectMetas.get(index++), meta);
        }
        assertEquals(expectMetas.size(), index);

        index = 0;
        itr = testingManifest.searchMetas(100, 660);
        while (itr.hasNext()) {
            SSTableFileMetaInfo meta = itr.next();
            assertEquals(expectMetas.get(index++), meta);
        }
        assertEquals(expectMetas.size(), index);

        index = 0;
        itr = testingManifest.searchMetas(110, 600);
        while (itr.hasNext()) {
            SSTableFileMetaInfo meta = itr.next();
            assertEquals(expectMetas.get(index++), meta);
        }
        assertEquals(expectMetas.size(), index);

        index = 0;
        itr = testingManifest.searchMetas(200, 600);
        while (itr.hasNext()) {
            SSTableFileMetaInfo meta = itr.next();
            assertEquals(expectMetas.get(index++), meta);
        }
        assertEquals(expectMetas.size(), index);
    }
}