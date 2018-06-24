package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
public class SSTableFileMetaInfo {
    private int fileNumber;
    private int firstIndex;
    private int lastIndex;
    private long fileSize;

    public void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    public void setFirstIndex(int firstIndex) {
        this.firstIndex = firstIndex;
    }

    public void setLastIndex(int lastIndex) {
        this.lastIndex = lastIndex;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public int getFileNumber() {
        return fileNumber;
    }

    public int getFirstIndex() {
        return firstIndex;
    }

    public int getLastIndex() {
        return lastIndex;
    }

    public long getFileSize() {
        return fileSize;
    }
}
