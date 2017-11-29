package raft.server.connections;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import raft.server.rpc.AppendEntries;
import raft.server.rpc.Request;

import java.net.InetSocketAddress;


/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemoteRaftClient {
    private final Bootstrap bootstrap;
    private ChannelFuture channelFuture;

    public RemoteRaftClient(EventLoopGroup eventLoopGroup) {
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
                p.addLast("raftHandler", new RaftRequestHandler(null));
            }
        });
    }

    public ChannelFuture connect(InetSocketAddress addr) {
        synchronized (this.bootstrap) {
            channelFuture = this.bootstrap.connect(addr);
        }
        return channelFuture;
    }

    public void send(Request req) {

    }

    public ChannelFuture ping() {
        AppendEntries ping = new AppendEntries();
        return channelFuture.channel().writeAndFlush("Ping");
    }

    public ChannelFuture close() {
        return channelFuture.channel().close();
    }

    @Override
    public String toString() {
        return "" + channelFuture.channel().remoteAddress();
    }
}
