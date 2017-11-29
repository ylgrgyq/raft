package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.nio.charset.Charset;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyDecoder extends MessageToMessageDecoder<ByteBuf> {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg, List<Object> out) throws Exception {
        out.add(msg.toString(Charset.defaultCharset()));
    }
}
