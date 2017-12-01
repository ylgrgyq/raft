package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import raft.server.rpc.Request;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyEncoder extends MessageToByteEncoder<Request> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Request msg, ByteBuf out) throws Exception {
        ByteBufAllocator alloc = ctx.alloc();
        ByteBuf buf = alloc.buffer(msg.getLength());
        msg.encode(buf);
        out.writeBytes(buf);
    }
}

