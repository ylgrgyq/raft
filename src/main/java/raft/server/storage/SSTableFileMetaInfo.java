package raft.server.storage;

import java.util.Objects;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class SSTableFileMetaInfo {
    private int fileNumber;
    private long firstKey;
    private long lastKey;
    private long fileSize;

    void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    void setFirstKey(long firstIndex) {
        this.firstKey = firstIndex;
    }

    void setLastKey(long lastIndex) {
        this.lastKey = lastIndex;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    int getFileNumber() {
        return fileNumber;
    }

    long getFirstKey() {
        return firstKey;
    }

    long getLastKey() {
        return lastKey;
    }

    long getFileSize() {
        return fileSize;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SSTableFileMetaInfo)) return false;
        SSTableFileMetaInfo that = (SSTableFileMetaInfo) o;
        return getFileNumber() == that.getFileNumber() &&
                getFirstKey() == that.getFirstKey() &&
                getLastKey() == that.getLastKey() &&
                getFileSize() == that.getFileSize();
    }

    @Override
    public int hashCode() {

        return Objects.hash(getFileNumber(), getFirstKey(), getLastKey(), getFileSize());
    }
}
