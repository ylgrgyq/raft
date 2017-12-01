package raft.server.rpc;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntries extends Request{
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;

    private long leaderCommit;

    public AppendEntries(RequestHeader header) {
        super(header);
    }

    @Override
    public void decode(ByteBuf buf) {

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
