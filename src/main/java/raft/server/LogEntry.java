package raft.server;

import java.nio.ByteBuffer;
import java.util.Base64;

/**
 * Author: ylgrgyq
 * Date: 17/12/25
 */
public class LogEntry {
    public static final LogEntry emptyEntry = new LogEntry();

    private int index;
    private int term;
    private byte[] data = new byte[0];

    public static byte[] encode(LogEntry entry){
        byte[] data = entry.getData();
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + Integer.BYTES + data.length);
        buffer.putInt(entry.getIndex());
        buffer.putInt(entry.getTerm());
        buffer.putInt(data.length);
        buffer.put(data);

        return buffer.array();
    }

    public static LogEntry decode(byte[] entryBytes) {
        ByteBuffer buffer = ByteBuffer.wrap(entryBytes);
        LogEntry entry = new LogEntry();
        entry.setIndex(buffer.getInt());
        entry.setTerm(buffer.getInt());

        int dataLength = buffer.getInt();
        byte[] data = new byte[dataLength];
        buffer.get(data);
        entry.setData(data);

        return entry;
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

    @Override
    public String toString() {
        return "LogEntry{" +
                "index=" + index +
                ", term=" + term +
                ", data=" + new String(Base64.getEncoder().encode(data)) +
                '}';
    }
}
