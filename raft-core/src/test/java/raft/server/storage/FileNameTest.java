package raft.server.storage;

import org.junit.Test;
import raft.server.TestUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/7/6
 */
public class FileNameTest {
    @Test
    public void setCurrentFile() throws Exception {
        String storageName = "testingStorage";
        String baseDir = "./target/storage";
        Path basePath = Paths.get(baseDir);
        TestUtil.cleanDirectory(basePath);
        int fileNumber = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        FileName.setCurrentFile(baseDir, storageName, fileNumber);

        assertEquals(FileName.getManifestFileName(storageName, fileNumber),
                new String(Files.readAllBytes(Paths.get(baseDir, FileName.getCurrentManifestFileName(storageName))),
                        StandardCharsets.UTF_8));
    }

    @Test
    public void parseFileName() throws Exception {
        String storageName = "testingStorage";

        String testingFileName;
        testingFileName = FileName.getCurrentManifestFileName(storageName);
        checkParseFileName(testingFileName, storageName, 0, FileName.FileType.Current);

        int testingFileNumber;
        testingFileNumber = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        testingFileName = FileName.getLogFileName(storageName, testingFileNumber);
        checkParseFileName(testingFileName, storageName, testingFileNumber, FileName.FileType.Log);

        testingFileNumber = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        testingFileName = FileName.getManifestFileName(storageName, testingFileNumber);
        checkParseFileName(testingFileName, storageName, testingFileNumber, FileName.FileType.Manifest);

        testingFileNumber = ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE);
        testingFileName = FileName.getSSTableName(storageName, testingFileNumber);
        checkParseFileName(testingFileName, storageName, testingFileNumber, FileName.FileType.SSTable);
    }

    private void checkParseFileName(String testingFileName, String expectStorageName,
                                    int expectFileNumber, FileName.FileType expectType) {
        String fullPath = "/home/user/hello/world/";
        String relativePath = "./user/hello/world/";

        FileName.FileNameMeta meta;
        meta = FileName.parseFileName(testingFileName);
        assertEquals(expectFileNumber, meta.getFileNumber());
        assertEquals(expectStorageName, meta.getStorageName());
        assertEquals(expectType, meta.getType());

        meta = FileName.parseFileName(fullPath + testingFileName);
        assertEquals(expectFileNumber, meta.getFileNumber());
        assertEquals(expectStorageName, meta.getStorageName());
        assertEquals(expectType, meta.getType());

        meta = FileName.parseFileName(relativePath + testingFileName);
        assertEquals(expectFileNumber, meta.getFileNumber());
        assertEquals(expectStorageName, meta.getStorageName());
        assertEquals(expectType, meta.getType());
    }
}