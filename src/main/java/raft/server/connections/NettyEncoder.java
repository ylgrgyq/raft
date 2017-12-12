package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Util;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyEncoder extends MessageToByteEncoder<RemotingCommand> {
    private static final Logger logger = LoggerFactory.getLogger(NettyEncoder.class.getName());

    @Override
    protected void encode(ChannelHandlerContext ctx, RemotingCommand msg, ByteBuf out) throws Exception {
        try {
            ByteBufAllocator alloc = ctx.alloc();
            ByteBuf buf = alloc.buffer(msg.getLength());
            buf = msg.encode(buf);
            out.writeBytes(buf);
            byte[] body = msg.getBody();
            if (body != null) {
                out.writeBytes(body);
            }
        } catch (Exception e) {
            logger.error("encode exception from {}", Util.parseChannelRemoteAddr(ctx.channel()), e);

            Util.closeChannel(ctx.channel());
        }
    }
}

