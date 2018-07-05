package raft.server.storage;

import raft.server.PersistentStateException;

import java.io.IOException;
import java.nio.file.*;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
public class FileName {
    public static String getCurrentManifestFileName(String storageName) {
        return storageName + "_CURRENT";
    }

    private static String generateFileName(String storageName, int fileNumber, String suffix) {
        return String.format("%s-%07d.%s", storageName, fileNumber, suffix);
    }

    static String getSSTableName(String storageName, int fileNumber){
        return generateFileName(storageName, fileNumber, "sst");
    }

    static String getLogFileName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "log");
    }

    static String getManifestFileName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "mf");
    }

    static void setCurrentFile(String baseDir, String storageName, int manifestFileNumber) throws IOException {
        Path tmpPath = Files.createTempFile(Paths.get(baseDir), storageName, ".tmp_mf");
        try {
            String manifestFileName = getManifestFileName(storageName, manifestFileNumber);

            Files.write(tmpPath, manifestFileName.getBytes(), StandardOpenOption.SYNC,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE);

            Files.move(tmpPath, Paths.get(baseDir, manifestFileName), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            Files.deleteIfExists(tmpPath);
            throw ex;
        }
    }
}
