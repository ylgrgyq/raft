package raft.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RequestVoteCommand extends RaftServerCommand {
    private int lastLogIndex = 0;
    private int lastLogTerm = 0;
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
        this.lastLogIndex = buf.getInt();
        this.lastLogTerm = buf.getInt();
        this.voteGranted = buf.get() == 1;
        return buf;
    }

    byte[] encode() {
        byte[] base = super.encode();

        ByteBuffer buf = ByteBuffer.allocate(base.length + Integer.BYTES + Integer.BYTES + Byte.BYTES);
        buf.put(base);
        buf.putInt(lastLogIndex);
        buf.putInt(lastLogTerm);
        buf.put((byte)(voteGranted ? 1 : 0));

        return buf.array();
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
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
