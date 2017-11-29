package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyEncoder extends MessageToByteEncoder<String> {
    @Override
    protected void encode(ChannelHandlerContext ctx, String msg, ByteBuf out) throws Exception {
        out.writeBytes((msg + "\n").getBytes());
    }
}

