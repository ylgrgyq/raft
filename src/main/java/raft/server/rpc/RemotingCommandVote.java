package raft.server.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemotingCommandVote implements SerializableCommand {
    private long candidateId;
    private long lastLogIndex;
    private long lastLogTerm;

    public void decode(byte[] bytes) {
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        this.candidateId = buf.getLong();
        this.lastLogIndex = buf.getLong();
        this.lastLogTerm = buf.getLong();
    }


    public byte[] encode() {
        ByteBuffer buf = ByteBuffer.allocate(24);
        buf.putLong(candidateId);
        buf.putLong(lastLogIndex);
        buf.putLong(lastLogTerm);

        return buf.array();
    }

    @Override
    public String toString() {
        return "RemotingCommandVote{" +
                "candidateId=" + candidateId +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                '}';
    }
}
