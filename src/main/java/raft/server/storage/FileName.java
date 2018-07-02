package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
public class FileName {
    private static int nextFileNumber = 1;

    public static String getCurrentManifestFileName(String storageName) {
        return storageName + "_CURRENT";
    }

    private static String generateFileName(String storageName, int fileNumber, String suffix) {
        return String.format("%s-%07d.%s", storageName, fileNumber, suffix);
    }

    public static synchronized int getNextFileNumber() {
        return nextFileNumber++;
    }

    public static String getSSTableName(String storageName, int fileNumber){
        return generateFileName(storageName, fileNumber, "sst");
    }

    public static String getLogFileName(String storageName, int fileNumber) {
        return generateFileName(storageName, fileNumber, "log");
    }
}
