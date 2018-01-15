package raft.server.rpc;

import raft.server.LogEntry;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntriesCommand extends RaftServerCommand {
    // so follower can redirect clients
    private String leaderId = "";
    // index of log entry immediately preceding new ones
    private int prevLogIndex = 0;
    // term of prevLogIndex entry
    private int prevLogTerm = 0;
    // leader’s commitIndex
    private int leaderCommit = 0;
    private boolean success = false;
    private LogEntry entry = LogEntry.emptyEntry;

    public AppendEntriesCommand(byte[] body) {
        this.setCode(CommandCode.APPEND_ENTRIES);
        this.decode(body);
    }

    public AppendEntriesCommand(int term, String leaderId) {
        super(term, CommandCode.APPEND_ENTRIES);
        this.leaderId = leaderId;
    }

    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = super.decode(bytes);

        int length = buf.getInt();
        assert length != 0 : "leaderId must not empty";
        byte[] leaderIdBytes = new byte[length];
        buf.get(leaderIdBytes);

        this.leaderId = new String(leaderIdBytes);
        this.prevLogIndex = buf.getInt();
        this.prevLogTerm = buf.getInt();
        this.leaderCommit = buf.getInt();
        this.success = buf.get() == 1;

        length = buf.getInt();
        byte[] entryBytes = new byte[length];
        buf.get(entryBytes);
        this.entry = LogEntry.decode(entryBytes);

        return buf;
    }

    public byte[] encode() {
        byte[] base = super.encode();

        byte[] leaderIdBytes = SerializableCommand.EMPTY_BYTES;
        if (this.leaderId != null) {
            leaderIdBytes = this.leaderId.getBytes(StandardCharsets.UTF_8);
        }

        byte[] entryBytes = LogEntry.encode(this.entry);

        ByteBuffer buffer = ByteBuffer.allocate(base.length +
                // leaderId
                Integer.BYTES + leaderIdBytes.length +
                // prevLogIndex
                Integer.BYTES +
                // prevLogTerm
                Integer.BYTES +
                // leaderCommit
                Integer.BYTES +
                // success
                Byte.BYTES +
                // entry
                Integer.BYTES + entryBytes.length);
        buffer.put(base);
        buffer.putInt(leaderIdBytes.length);
        buffer.put(leaderIdBytes);
        buffer.putLong(this.prevLogIndex);
        buffer.putLong(this.prevLogTerm);
        buffer.putLong(this.leaderCommit);
        buffer.put((byte)(success ? 1 : 0));
        buffer.putInt(entryBytes.length);
        buffer.put(entryBytes);

        return buffer.array();
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public Optional<LogEntry> getEntry() {
        if (this.entry == LogEntry.emptyEntry) {
            return Optional.empty();
        }
        return Optional.of(entry);
    }

    public void setEntry(LogEntry entry) {
        this.entry = entry;
    }

    @Override
    public String toString() {
        return "AppendEntriesCommand{" +
                "leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", leaderCommit=" + leaderCommit +
                ", success=" + success +
                ", entry=" + entry.toString() +
                '}';
    }
}
