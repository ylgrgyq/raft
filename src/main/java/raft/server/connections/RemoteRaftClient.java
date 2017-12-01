package raft.server.connections;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import raft.server.RaftServer;
import raft.server.Util;
import raft.server.rpc.AppendEntries;
import raft.server.rpc.Request;
import raft.server.rpc.RequestCode;
import raft.server.rpc.RequestHeader;

import java.net.InetSocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemoteRaftClient {
    private final Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private String id;
    private RaftServer server;

    public RemoteRaftClient(final EventLoopGroup eventLoopGroup, final RaftServer server) {
        this.server = server;
        this.bootstrap = new Bootstrap();

        if (eventLoopGroup != null){
            this.bootstrap.group(eventLoopGroup);
        } else {
            this.bootstrap.group(new NioEventLoopGroup(1));
        }
        this.bootstrap.channel(NioSocketChannel.class);
        this.bootstrap.option(ChannelOption.TCP_NODELAY, true);
        this.bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(final SocketChannel channel) throws Exception {
                final ChannelPipeline p = channel.pipeline();
                p.addLast(new NettyEncoder());
                p.addLast(new NettyDecoder());
                p.addLast(server.getHandler());
            }
        });
    }

    public ChannelFuture connect(InetSocketAddress addr) {
        synchronized (this.bootstrap) {
            channelFuture = this.bootstrap.connect(addr);
        }
        return channelFuture;
    }

    public String getId() {
        if (this.id == null) {
            this.id = Util.parseChannelRemoteAddr(channelFuture.channel());
        }
        return this.id;
    }

    public void send(Request req) {

    }

    public ChannelFuture ping() {

        RequestHeader header = new RequestHeader();
        header.setTerm(server.getTerm());
        header.setRequestCode(RequestCode.APPEND_ENTRIES);
        AppendEntries ping = new AppendEntries(header);
        return channelFuture.channel().writeAndFlush(ping);
    }

    public ChannelFuture close() {
        return channelFuture.channel().close();
    }

    @Override
    public String toString() {
        return "" + channelFuture.channel().remoteAddress();
    }
}
