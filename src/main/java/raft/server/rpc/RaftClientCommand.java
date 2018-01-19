package raft.server.rpc;

import raft.server.LogEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public class RaftClientCommand extends RaftCommand {
    private LogEntry entry = LogEntry.emptyEntry;
    private String leaderId = "";
    private boolean success = false;

    public RaftClientCommand(byte[] body){
        this.setCode(CommandCode.CLIENT_REQUEST);
        this.decode(body);
    }

    public RaftClientCommand() {
        super(CommandCode.REQUEST_VOTE);
    }

    @Override
    byte[] encode() {
        byte[] base = super.encode();

        byte[] entryBytes = this.entry.encode();
        byte[] leaderIdBytes = this.leaderId.getBytes(StandardCharsets.UTF_8);

        ByteBuffer buffer = ByteBuffer.allocate(base.length +
                // log entry
                entryBytes.length +
                // leader id
                Integer.BYTES +
                leaderIdBytes.length +
                // success
                Byte.BYTES);
        buffer.put(base);

        // log entry
        buffer.put(entryBytes);

        // leader id
        buffer.putInt(leaderIdBytes.length);
        buffer.put(leaderIdBytes);

        // success
        buffer.put(this.success ? (byte) 1 : (byte) 0);

        return buffer.array();
    }

    @Override
    ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = super.decode(bytes);

        // log entry
        this.entry = LogEntry.from(buf);

        // leader id
        int leaderIdLength = buf.getInt();
        byte[] leaderId = new byte[leaderIdLength];
        buf.get(leaderId);
        this.leaderId = new String(leaderId);

        // success
        this.success = buf.get() == 1;

        return buf;
    }

    @Override
    public String toString() {
        return "RaftClientCommand{" +
                "entry=" + this.entry.toString() +
                ", leaderId='" + leaderId + '\'' +
                ", success=" + success +
                '}';
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId == null ? "" : leaderId;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public LogEntry getEntry() {
        return entry;
    }

    public void setEntry(LogEntry entry) {
        this.entry = entry;
    }
}
