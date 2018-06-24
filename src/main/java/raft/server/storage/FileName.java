package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
public class FileName {
    private static int nextFileNumber = 1;

    public static String getCurrentManifestFileName(String dbName) {
        return dbName + "_CURRENT";
    }

    private static String generateFileName(String dbName, int fileNumber, String suffix) {
        return String.format("%s-%07d.%s", dbName, fileNumber, suffix);
    }

    public static int getNextFileNumber() {
        return nextFileNumber++;
    }

    public static String getSSTableName(String dbName, int fileNumber){
        return generateFileName(dbName, fileNumber, "sst");
    }

    public static void main(String[] args) {
        System.out.println(FileName.getSSTableName("hahaha", 23));
    }
}
