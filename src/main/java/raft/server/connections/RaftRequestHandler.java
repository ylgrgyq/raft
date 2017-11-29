package raft.server.connections;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import raft.server.RaftServer;

/**
 * Author: ylgrgyq
 * Date: 17/11/23
 */
public class RaftRequestHandler extends SimpleChannelInboundHandler<String> {
    private RaftServer server;

    public RaftRequestHandler(RaftServer server) {
        this.server = server;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String o) throws Exception {
        System.out.println("Receive msg: " + o);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
        System.out.println("Got exception" + cause.getMessage());
        cause.printStackTrace();
    }
}
