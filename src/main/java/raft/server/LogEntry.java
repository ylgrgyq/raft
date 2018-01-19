package raft.server;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Author: ylgrgyq
 * Date: 17/12/25
 */
public class LogEntry {
    public static final LogEntry emptyEntry = new LogEntry();

    //TODO reduce encoded bytes size for empty entry
    //TODO do not encode empty entry repeatedly
    private int index = 0;
    private int term = 0;
    private byte[] data = new byte[0];

    public static LogEntry from(ByteBuffer buffer) {
        LogEntry entry = new LogEntry();
        entry.setIndex(buffer.getInt());
        entry.setTerm(buffer.getInt());

        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);
        entry.setData(data);

        return entry;
    }

    public byte[] encode(){
        ByteBuffer buffer = ByteBuffer.allocate(this.getSize());
        buffer.putInt(this.getIndex());
        buffer.putInt(this.getTerm());
        byte[] data = this.getData();
        buffer.putInt(data.length);
        buffer.put(data);

        return buffer.array();
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public byte[] getData() {
        return data;
    }

    public void setData(byte[] data) {
        this.data = data;
    }

    public int getSize() {
        return Integer.BYTES + Integer.BYTES + Integer.BYTES + data.length;
    }

    @Override
    public String toString() {
        return "LogEntry{" +
                "index=" + index +
                ", term=" + term +
                ", data=" + new String(Base64.getEncoder().encode(data)) +
                '}';
    }
}
