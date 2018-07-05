package raft.server.storage;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class FileName {
    static String getCurrentManifestFileName(String storageName) {
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

            Files.write(tmpPath, manifestFileName.getBytes(StandardCharsets.UTF_8), StandardOpenOption.SYNC,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE);

            Files.move(tmpPath, Paths.get(baseDir, getCurrentManifestFileName(storageName)), StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            Files.deleteIfExists(tmpPath);
            throw ex;
        }
    }

    static FileNameMeta parseFileName(String fileName) {
        if (fileName.startsWith("/") || fileName.startsWith("./")) {
            String[] strs = fileName.split("/");
            assert strs.length > 0;
            fileName = strs[strs.length - 1];
        }

        if (fileName.endsWith("_CURRENT")) {
            String[] strs = fileName.split("_");
            assert strs.length == 2;
            return new FileNameMeta(strs[0], 0, FileType.Current);
        } else {
            String[] strs = fileName.split("[\\-\\.]",3);
            assert strs.length == 3;
            String storageName = strs[0];
            int fileNumber = Integer.valueOf(strs[1]);
            FileType type = FileType.Unknown;
            switch (strs[2]) {
                case "sst":
                    type = FileType.SSTable;
                    break;
                case "log":
                    type = FileType.Log;
                    break;
                case "mf":
                    type = FileType.Manifest;
                    break;
            }
            return new FileNameMeta(storageName, fileNumber, type);
        }
    }

    static class FileNameMeta {
        private final String storageName;
        private final int fileNumber;
        private final FileType type;

        FileNameMeta(String storageName, int fileNumber, FileType type) {
            this.storageName = storageName;
            this.fileNumber = fileNumber;
            this.type = type;
        }

        String getStorageName() {
            return storageName;
        }

        int getFileNumber() {
            return fileNumber;
        }

        FileType getType() {
            return type;
        }
    }

    enum FileType {
        Unknown,
        SSTable,
        Current,
        Log,
        Manifest
    }
}
