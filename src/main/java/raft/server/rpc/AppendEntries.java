package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntries implements SerializableCommand {
    private String leaderId = null;
    private long prevLogIndex = -1;
    private long prevLogTerm = -1;

    private long leaderCommit = -1;

    public void decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int leaderIdLength = buf.getInt();
        byte[] leaderIdBytes = new byte[leaderIdLength];
        buf.get(leaderIdBytes);
        this.leaderId = new String(leaderIdBytes);
        this.prevLogIndex = buf.getLong();
        this.prevLogTerm = buf.getLong();
        this.leaderCommit = buf.getLong();
    }

    public byte[] encode() {
        byte[] leaderIdBytes = new byte[0];
        if (leaderId != null) {
            leaderIdBytes = leaderId.getBytes(StandardCharsets.UTF_8);
        }

        ByteBuffer buffer = ByteBuffer.allocate(4 + leaderIdBytes.length + 24);
        buffer.putInt(leaderIdBytes.length);
        buffer.put(leaderIdBytes);
        buffer.putLong(this.prevLogIndex);
        buffer.putLong(this.prevLogTerm);
        buffer.putLong(this.leaderCommit);

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

    @Override
    public String toString() {
        return "AppendEntries{" +
                "leaderId='" + leaderId + '\'' +
                ", prevLogIndex=" + prevLogIndex +
                ", prevLogTerm=" + prevLogTerm +
                ", leaderCommit=" + leaderCommit +
                '}';
    }
}
