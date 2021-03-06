package raft.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class FileName {
    private static final Logger logger = LoggerFactory.getLogger(FileName.class.getName());

    static String getCurrentManifestFileName(String storageName) {
        return storageName + "_CURRENT";
    }

    private static String generateFileName(String storageName, int fileNumber, String suffix) {
        return String.format("%s-%07d.%s", storageName, fileNumber, suffix);
    }

    static String getLockFileName(String storageName) {
        return storageName + ".lock";
    }

    static String getSSTableName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "sst");
    }

    static String getLogFileName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "log");
    }

    static String getManifestFileName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "mf");
    }

    static void setCurrentFile(String baseDir, String storageName, int manifestFileNumber) throws IOException {
        Path tmpPath = Files.createTempFile(Paths.get(baseDir), storageName + "_", ".tmp_mf");
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
        assert fileName != null;
        if (fileName.startsWith("/") || fileName.startsWith("./")) {
            String[] strs = fileName.split("/");
            assert strs.length > 0;
            fileName = strs[strs.length - 1];
        }

        FileType type = FileType.Unknown;
        int fileNumber = 0;
        String storageName = "";
        if (fileName.endsWith("_CURRENT")) {
            storageName = fileName.substring(0, fileName.length() - "_CURRENT".length());
            type = FileType.Current;
        } else if (fileName.endsWith(".lock")) {
            storageName = fileName.substring(0, fileName.length() - ".lock".length());
            type = FileType.Lock;
        } else if (fileName.endsWith(".tmp_mf")) {
            storageName = fileName.substring(0, fileName.length() - ".tmp_mf".length());
            type = FileType.TempManifest;
        } else {
            String[] strs = fileName.split("[\\-.]", 3);
            if (strs.length == 3) {
                storageName = strs[0];
                fileNumber = Integer.parseInt(strs[1]);
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
                    default:
                        break;
                }
            }
        }
        return new FileNameMeta(fileName, storageName, fileNumber, type);
    }

    private static List<Path> getOutdatedFiles(String baseDir, int logFileNumber, TableCache tableCache) {
        try {
            File dirFile = new File(baseDir);
            File[] files = dirFile.listFiles();

            if (files != null) {
                return Arrays.stream(files)
                        .filter(File::isFile)
                        .map(File::getName)
                        .map(FileName::parseFileName)
                        .filter(meta -> {
                            switch (meta.getType()) {
                                case Current:
                                case Lock:
                                case TempManifest:
                                case Manifest:
                                    return false;
                                case Unknown:
                                    return false;
                                case Log:
                                    return meta.getFileNumber() < logFileNumber;
                                case SSTable:
                                    return ! tableCache.hasTable(meta.getFileNumber());
                                default:
                                    return false;
                            }
                        })
                        .map(meta -> Paths.get(baseDir, meta.getFileName()))
                        .collect(Collectors.toList());
            } else {
                return Collections.emptyList();
            }
        } catch (Throwable ex) {
            logger.error("get outdated files under dir:{} failed", baseDir, ex);
            return Collections.emptyList();
        }
    }

    static void deleteOutdatedFiles(String baseDir, int logFileNumber, TableCache tableCache) {
        List<Path> outdatedFilePaths = getOutdatedFiles(baseDir, logFileNumber, tableCache);
        try {
            for (Path path : outdatedFilePaths) {
                Files.deleteIfExists(path);
            }
        } catch (Throwable t) {
            logger.error("delete outdated files:{} failed", outdatedFilePaths, t);
        }
    }

    static class FileNameMeta {
        private final String fileName;
        private final String storageName;
        private final int fileNumber;
        private final FileType type;

        FileNameMeta(String fileName, String storageName, int fileNumber, FileType type) {
            this.fileName = fileName;
            this.storageName = storageName;
            this.fileNumber = fileNumber;
            this.type = type;
        }

        String getFileName() {
            return fileName;
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
        Manifest,
        TempManifest,
        Lock
    }
}
