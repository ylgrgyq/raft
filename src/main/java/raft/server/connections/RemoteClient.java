package raft.server.connections;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Util;
import raft.server.RemoteServer;
import raft.server.rpc.PendingRequestCallback;
import raft.server.rpc.RemotingCommand;

import java.net.InetSocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemoteClient {
    private static final Logger logger = LoggerFactory.getLogger(RemoteClient.class.getName());

    private final Bootstrap bootstrap;
    private ChannelFuture channelFuture;
    private String id;
    private RemoteServer server;
    private volatile boolean closed;

    public RemoteClient(final EventLoopGroup eventLoopGroup, final RemoteServer server) {
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

    private Future<Void> doSend(RemotingCommand cmd) {
        ChannelFuture future = channelFuture.channel().writeAndFlush(cmd);
        future.addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("send request to {} failed", this, f.cause());
                if (!cmd.isOneWay()) {
                    this.server.removePendingRequest(cmd.getRequestId());
                }
                this.close();
            }
        });

        return future;
    }

    public Future<Void> send(RemotingCommand cmd, PendingRequestCallback callback) {
        this.server.addPendingRequest(cmd.getRequestId(), 3000, callback);
        return doSend(cmd);
    }

    public ChannelFuture close() {
        this.closed = true;
        return channelFuture.channel().close();
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public String toString() {
        return "" + channelFuture.channel().remoteAddress();
    }
}
