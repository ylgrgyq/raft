package raft.server.storage;

import com.google.common.io.ByteArrayDataInput;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class ManifestRecord {
    private int nextFileNumber;
    private int logNumber;
    private final List<SSTableFileMetaInfo> metas;

    ManifestRecord() {
        this.metas = new ArrayList<>();
    }

    int getNextFileNumber() {
        return nextFileNumber;
    }

    void setNextFileNumber(int nextFileNumber) {
        this.nextFileNumber = nextFileNumber;
    }

    int getLogNumber() {
        return logNumber;
    }

    void setLogNumber(int logNumber) {
        this.logNumber = logNumber;
    }

    List<SSTableFileMetaInfo> getMetas() {
        return metas;
    }

    void addMeta(SSTableFileMetaInfo meta) {
        assert meta != null;
        metas.add(meta);
    }

    byte[] encode(){
        ByteArrayDataOutput out = ByteStreams.newDataOutput();
        out.writeInt(nextFileNumber);
        out.writeInt(logNumber);

        out.writeInt(metas.size());
        for (SSTableFileMetaInfo meta : metas) {
            out.writeLong(meta.getFileSize());
            out.writeInt(meta.getFileNumber());
            out.writeInt(meta.getFirstKey());
            out.writeInt(meta.getLastKey());
        }

        return out.toByteArray();
    }

    static ManifestRecord decode(byte[] bytes) {
        ManifestRecord record = new ManifestRecord();

        ByteArrayDataInput in = ByteStreams.newDataInput(bytes);
        record.setNextFileNumber(in.readInt());
        record.setLogNumber(in.readInt());

        int metasSize = in.readInt();
        for (int i = 0; i < metasSize; i++) {
            SSTableFileMetaInfo meta = new SSTableFileMetaInfo();
            meta.setFileSize(in.readLong());
            meta.setFileNumber(in.readInt());
            meta.setFirstKey(in.readInt());
            meta.setLastKey(in.readInt());

            record.addMeta(meta);
        }

        return record;
    }
}
