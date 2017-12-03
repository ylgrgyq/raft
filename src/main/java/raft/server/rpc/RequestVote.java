package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RequestVote implements SerializableCommand {
    private String candidateId = "";
    private long lastLogIndex = -1;
    private long lastLogTerm = -1;

    public void decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        int idLength = buf.getInt();
        byte[] idBytes = new byte[idLength];
        buf.get(idBytes);
        this.candidateId = new String(idBytes);
        this.lastLogIndex = buf.getLong();
        this.lastLogTerm = buf.getLong();
    }


    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(24);
        byte[] idBytes = SerializableCommand.EMPTY_BYTES;
        if (candidateId != null) {
            idBytes = candidateId.getBytes(StandardCharsets.UTF_8);
        }
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putLong(lastLogIndex);
        buf.putLong(lastLogTerm);

        return buf.array();
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public long getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(long lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(long lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    @Override
    public String toString() {
        return "RequestVote{" +
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
