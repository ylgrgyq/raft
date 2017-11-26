package raft.server.connections;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import raft.server.RaftServer;

/**
 * Author: ylgrgyq
 * Date: 17/11/23
 */
public class RaftRequestHandler extends SimpleChannelInboundHandler<Object> {
    private RaftServer server;

    public RaftRequestHandler(RaftServer server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext channelHandlerContext, Object o) throws Exception {

    }
}
