package raft.server.rpc;

import io.netty.buffer.ByteBuf;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RequestVote extends Request {
    private long candidateId;
    private long lastLogIndex;
    private long lastLogTerm;

    public RequestVote(RequestHeader header) {
        super(header);
    }

    @Override
    public void decode(ByteBuf buf) {
        this.candidateId = buf.readLong();
        this.lastLogIndex = buf.readLong();
        this.lastLogTerm = buf.readLong();
    }

    @Override
    protected ByteBuf encode0(ByteBuf buf) {
        return null;
    }

    @Override
    public int getLength() {
        return 0;
    }
}
