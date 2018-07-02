package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class SSTableFileMetaInfo {
    private int fileNumber;
    private int firstKey;
    private int lastKey;
    private long fileSize;

    void setFileNumber(int fileNumber) {
        this.fileNumber = fileNumber;
    }

    void setFirstKey(int firstIndex) {
        this.firstKey = firstIndex;
    }

    void setLastKey(int lastIndex) {
        this.lastKey = lastIndex;
    }

    void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    int getFileNumber() {
        return fileNumber;
    }

    int getFirstKey() {
        return firstKey;
    }

    int getLastKey() {
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

        if (fileNumber != that.fileNumber) return false;
        if (firstKey != that.firstKey) return false;
        if (lastKey != that.lastKey) return false;
        return fileSize == that.fileSize;
    }

    @Override
    public int hashCode() {
        int result = fileNumber;
        result = 31 * result + firstKey;
        result = 31 * result + lastKey;
        result = 31 * result + (int) (fileSize ^ (fileSize >>> 32));
        return result;
    }
}
