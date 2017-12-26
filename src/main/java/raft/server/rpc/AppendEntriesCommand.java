package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntriesCommand extends RaftServerCommand {
    // so follower can redirect clients
    private String leaderId = "";
    // index of log entry immediately preceding new ones
    private long prevLogIndex = -1;
    // term of prevLogIndex entry
    private long prevLogTerm = -1;
    // leader’s commitIndex
    private long leaderCommit = -1;
    private boolean success = false;
    // currentTerm, for leader to update itself
    private int term;

    public AppendEntriesCommand(byte[] body) {
        this.setCode(CommandCode.APPEND_ENTRIES);
        this.decode(body);
    }

    public AppendEntriesCommand(int term) {
        super(term, CommandCode.APPEND_ENTRIES);
    }

    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = super.decode(bytes);

        int leaderIdLength = buf.getInt();
        byte[] leaderIdBytes = new byte[leaderIdLength];
        buf.get(leaderIdBytes);
        this.leaderId = new String(leaderIdBytes);
        this.prevLogIndex = buf.getLong();
        this.prevLogTerm = buf.getLong();
        this.leaderCommit = buf.getLong();
        this.success = buf.get() == 1;

        return buf;
    }

    public byte[] encode() {
        byte[] base = super.encode();

        byte[] leaderIdBytes = SerializableCommand.EMPTY_BYTES;
        if (leaderId != null) {
            leaderIdBytes = leaderId.getBytes(StandardCharsets.UTF_8);
        }

        ByteBuffer buffer = ByteBuffer.allocate(base.length + 4 + leaderIdBytes.length + 8 + 8 + 8 + 1);
        buffer.put(base);
        buffer.putInt(leaderIdBytes.length);
        buffer.put(leaderIdBytes);
        buffer.putLong(this.prevLogIndex);
        buffer.putLong(this.prevLogTerm);
        buffer.putLong(this.leaderCommit);
        buffer.put((byte)(success ? 1 : 0));

        return buffer.array();
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public long getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(long prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public long getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(long prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public long getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(long leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public boolean isSuccess() {
        return success;
    }

    public void markSuccess() {
        this.success = true;
    }

    @Override
    public String toString() {
        return "AppendEntriesCommand{" +
                "leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", leaderCommit=" + leaderCommit +
                ", success=" + success +
                '}';
    }
}
