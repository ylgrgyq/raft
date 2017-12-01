package raft.server.connections;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.Util;
import raft.server.rpc.AppendEntries;
import raft.server.rpc.RequestHeader;
import raft.server.rpc.RequestVote;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class NettyDecoder extends LengthFieldBasedFrameDecoder {
    private static Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());
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

            RequestHeader header = RequestHeader.decode(frame);
            switch (header.getRequestCode()) {
                case REQUEST_VOTE:
                    RequestVote vote = new RequestVote(header);
                    vote.decode(frame);
                    return vote;
                case APPEND_ENTRIES:
                    AppendEntries entry = new AppendEntries(header);
                    entry.decode(frame);
                    return entry;
            }
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
