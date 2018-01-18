package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RequestVoteCommand extends RaftServerCommand {
    private long lastLogIndex = -1;
    private long lastLogTerm = -1;
    private boolean voteGranted = false;

    public RequestVoteCommand(byte[] body){
        this.setCode(CommandCode.APPEND_ENTRIES);
        this.decode(body);
    }

    public RequestVoteCommand(int term, String candidateId) {
        super(term, candidateId, CommandCode.REQUEST_VOTE);
    }

    ByteBuffer decode(byte[] bytes) {
        ByteBuffer buf = super.decode(bytes);
        this.lastLogIndex = buf.getLong();
        this.lastLogTerm = buf.getLong();
        this.voteGranted = buf.get() == 1;
        return buf;
    }

    byte[] encode() {
        byte[] base = super.encode();

        ByteBuffer buf = ByteBuffer.allocate(base.length + Long.BYTES + Long.BYTES + Byte.BYTES);
        buf.put(base);
        buf.putLong(lastLogIndex);
        buf.putLong(lastLogTerm);
        buf.put((byte)(voteGranted ? 1 : 0));

        return buf.array();
    }

    public String getCandidateId() {
        return this.getFrom();
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

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    @Override
    public String toString() {
        return "RequestVoteCommand{" +
                "candidateId='" + this.getFrom() + '\'' +
                ", term=" + this.getTerm() +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", voteGranted=" + voteGranted +
                '}';
    }
}
