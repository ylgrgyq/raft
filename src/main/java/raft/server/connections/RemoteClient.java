package raft.server.connections;

import com.google.common.base.Preconditions;
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
import java.util.concurrent.TimeUnit;

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
    private InetSocketAddress clientAddr;

    public RemoteClient(final RemoteServer server, final InetSocketAddress addr) {
        this(new NioEventLoopGroup(1), server, addr);
    }

    public RemoteClient(final EventLoopGroup eventLoopGroup, final RemoteServer server, final InetSocketAddress addr) {
        Preconditions.checkNotNull(eventLoopGroup);
        Preconditions.checkNotNull(server);
        Preconditions.checkNotNull(addr);

        this.server = server;
        this.clientAddr = addr;
        this.id = Util.parseSocketAddressToString(addr);

        this.bootstrap = new Bootstrap();
        this.bootstrap.group(eventLoopGroup);
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

    private void scheduleReconnectToClientJob(ChannelFuture chFuture, long clientReconnectDelayMillis) {
        chFuture.channel().eventLoop().schedule(() -> this.connect(clientReconnectDelayMillis),
                clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
    }

    public ChannelFuture connect(long clientReconnectDelayMillis) {
        InetSocketAddress addr = this.clientAddr;
        synchronized (this.bootstrap) {
            this.channelFuture = this.bootstrap.connect(addr);
            this.channelFuture.addListener(f -> {
                if (f.isSuccess()) {
                    logger.info("connect to {} success", addr);
                    this.channelFuture.channel().closeFuture().addListener(cf -> {
                        logger.warn("connection with {} lost, start reconnect after {} millis", addr, clientReconnectDelayMillis);

                        // TODO Handle connection lost. maybe we need to pause RaftPeerNode when it's connection was lost
                        scheduleReconnectToClientJob(this.channelFuture, clientReconnectDelayMillis);
                    });
                } else {
                    logger.warn("connect to {} failed, start reconnect after {} millis", addr, clientReconnectDelayMillis);
                    scheduleReconnectToClientJob(this.channelFuture, clientReconnectDelayMillis);
                }
            });
        }
        return channelFuture;
    }

    public String getId() {
        if (this.id == null) {
            this.id = Util.parseChannelRemoteAddrToString(channelFuture.channel());
        }
        return this.id;
    }

    private Future<Void> doSend(RemotingCommand cmd) {
        Channel ch = channelFuture.channel();
        if (ch.isActive()) {
            logger.debug("send remoting command {} to {}", cmd, this.id);
            ChannelFuture future = ch.writeAndFlush(cmd);
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    logger.warn("send request to {} failed", RemoteClient.this, f.cause());
                    if (!cmd.isOneWay()) {
                        this.server.removePendingRequest(cmd.getRequestId());
                    }
                    this.close();
                }
            });
            return future;
        }

        if (!cmd.isOneWay()) {
            this.server.removePendingRequest(cmd.getRequestId());
        }
        return ch.eventLoop().newFailedFuture(new RuntimeException("connection lost"));
    }

    public Future<Void> send(RemotingCommand cmd, PendingRequestCallback callback) {
        this.server.addPendingRequest(cmd.getRequestId(), 3000, callback);
        return doSend(cmd);
    }

    public Future<Void> sendOneway(RemotingCommand cmd) {
        cmd.markOneWay(true);
        return doSend(cmd);
    }

    public ChannelFuture close() {
        return Util.closeChannel(channelFuture.channel());
    }

    @Override
    public String toString() {
        return "" + channelFuture.channel().remoteAddress();
    }
}
