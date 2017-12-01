package raft.server.rpc;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public abstract class Request {
    private RequestHeader header;

    Request(RequestHeader header) {
        this.header = header;
    }

    public abstract void decode(ByteBuf buf);

    public ByteBuf encode(ByteBuf buf){
        int length = this.getLength();

        buf = this.encode0(buf);

        return buf;
    }

    abstract protected ByteBuf encode0(ByteBuf buf);

    public abstract int getLength();
}
