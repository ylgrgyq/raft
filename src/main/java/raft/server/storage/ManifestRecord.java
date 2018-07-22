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
    private Type type;
    private final List<SSTableFileMetaInfo> metas;

    private ManifestRecord(Type type) {
        this.metas = new ArrayList<>();
        this.type = type;
    }

    static ManifestRecord newPlainRecord(){
        return new ManifestRecord(Type.PLAIN);
    }

    static ManifestRecord newReplaceAllExistedMetasRecord() {
        return new ManifestRecord(Type.REPLEASE_METAS);
    }

    int getNextFileNumber() {
        if (type == Type.PLAIN) {
            return nextFileNumber;
        } else {
            return -1;
        }
    }

    void setNextFileNumber(int nextFileNumber) {
        assert type == Type.PLAIN;
        this.nextFileNumber = nextFileNumber;
    }

    int getLogNumber() {
        if (type == Type.PLAIN) {
            return logNumber;
        } else {
            return -1;
        }
    }

    void setLogNumber(int logNumber) {
        assert type == Type.PLAIN;
        this.logNumber = logNumber;
    }

    Type getType() {
        return type;
    }

    List<SSTableFileMetaInfo> getMetas() {
        return metas;
    }

    void addMeta(SSTableFileMetaInfo meta) {
        assert meta != null;
        metas.add(meta);
    }

    void addMetas(List<SSTableFileMetaInfo> ms) {
        assert ms != null && ! ms.isEmpty();
        metas.addAll(ms);
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
        ManifestRecord record = new ManifestRecord(Type.PLAIN);

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

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder("ManifestRecord{" +
                "type=" + type +
                ", nextFileNumber=" + nextFileNumber +
                ", logNumber=" + logNumber);

        if (!metas.isEmpty()) {
            int from = metas.get(0).getFirstKey();
            int to = metas.get(metas.size() - 1).getLastKey();
            builder.append(", metaKeysFrom=");
            builder.append(from);
            builder.append(", metaKeysTo=");
            builder.append(to);
        }

        builder.append("}");

        return builder.toString();
    }

    enum Type {
        PLAIN,
        REPLEASE_METAS
    }
}
