package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RequestVoteCommand extends RaftServerCommand {
    private String candidateId = "";
    private long lastLogIndex = -1;
    private long lastLogTerm = -1;
    private boolean voteGranted = false;

    public RequestVoteCommand(byte[] body){
        this.decode(body);
    }

    public RequestVoteCommand(int term) {
        super(term, CommandCode.REQUEST_VOTE);
    }

    public ByteBuffer decode(byte[] bytes) {
        ByteBuffer buf = super.decode(bytes);
        int idLength = buf.getInt();
        byte[] idBytes = new byte[idLength];
        buf.get(idBytes);
        this.candidateId = new String(idBytes);
        this.lastLogIndex = buf.getLong();
        this.lastLogTerm = buf.getLong();
        this.voteGranted = buf.get() == 1;
        return buf;
    }

    public byte[] encode() {
        byte[] base = super.encode();

        byte[] idBytes = SerializableCommand.EMPTY_BYTES;
        if (candidateId != null) {
            idBytes = candidateId.getBytes(StandardCharsets.UTF_8);
        }

        ByteBuffer buf = ByteBuffer.allocate(base.length + 4 + idBytes.length + 8 + 8 + 1);
        buf.putInt(idBytes.length);
        buf.put(idBytes);
        buf.putLong(lastLogIndex);
        buf.putLong(lastLogTerm);
        buf.put((byte)(voteGranted ? 1 : 0));

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

    public boolean isVoteGranted() {
        return voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteCommand{" +
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
