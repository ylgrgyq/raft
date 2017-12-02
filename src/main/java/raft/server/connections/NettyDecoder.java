package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.Util;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static Logger logger = LoggerFactory.getLogger(NettyDecoder.class.getName());

    private static final int FRAME_MAX_LENGTH =
            Integer.parseInt(System.getProperty("raft.server.frameMaxLength", "16777216"));

    public NettyDecoder() {
        super(FRAME_MAX_LENGTH, 0, 4, 0, 4);
    }

    @Override
    protected Object decode(ChannelHandlerContext ctx, ByteBuf in) throws Exception {
        ByteBuf frame = null;
        try {
            frame = (ByteBuf) super.decode(ctx, in);
            if (frame == null) {
                return null;
            }

            return RemotingCommand.decode(frame);
        } catch (Exception e) {
            logger.error("decode exception from {}", Util.parseChannelRemoteAddr(ctx.channel()), e);
        } finally {
            if (frame != null) {
                frame.release();
            }
        }

        return null;
    }
}
