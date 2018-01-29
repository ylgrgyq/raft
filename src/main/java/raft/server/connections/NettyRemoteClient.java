package raft.server.connections;

import com.google.common.base.Preconditions;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.Util;
import raft.server.rpc.PendingRequest;
import raft.server.rpc.PendingRequestCallback;
import raft.server.rpc.RemotingCommand;

import java.net.SocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 18/1/25
 */
public class NettyRemoteClient {
    private static final Logger logger = LoggerFactory.getLogger(NettyRemoteClient.class.getName());

    private final ConcurrentHashMap<Integer, PendingRequest> pendingRequestTable = new ConcurrentHashMap<>();
    private final Bootstrap bootstrap;
    private final ScheduledExecutorService timer;
    private final ExecutorService callbackExecutor;
    private final ConcurrentHashMap<String, ChannelFuture> channelMap = new ConcurrentHashMap<>();
    private final EventLoopGroup eventLoopGroup;
    private final long clientReconnectDelayMillis;

    public NettyRemoteClient() {
        this(new NioEventLoopGroup(1), 1000);
    }

    public NettyRemoteClient(final EventLoopGroup eventLoopGroup, final long clientReconnectDelayMillis) {
        Preconditions.checkNotNull(eventLoopGroup);

        this.clientReconnectDelayMillis = clientReconnectDelayMillis;
        this.eventLoopGroup = eventLoopGroup;
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
                p.addLast(new RemoteClientHandler());
            }
        });

        this.callbackExecutor = Executors.newFixedThreadPool(4,
                new ThreadFactoryImpl("RemoteServerCallbackPoll_"));

        this.timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RaftServerTimer_"));
        this.timer.scheduleWithFixedDelay(this::scanPendingRequestTable, 2, 2, TimeUnit.SECONDS);
    }

    private void scheduleReconnectToClientJob(final EventLoop scheduler, final String addr) {
        scheduler.schedule(() -> this.connect(addr), this.clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
    }

    private void connect(final String addr) {
        final SocketAddress socketAddress = Util.parseStringToSocketAddress(addr);
        final ChannelFuture channelFuture = this.bootstrap.connect(socketAddress);
        final Channel ch = channelFuture.channel();
        final EventLoop scheduler = ch.eventLoop();
        this.channelMap.put(addr, channelFuture);
        channelFuture.addListener(f -> {
            if (f.isSuccess()) {
                logger.info("connect to {} success", addr);
                ch.closeFuture().addListener(cf -> {
                    logger.warn("connection with {} was lost, start reconnect after {} millis", addr, this.clientReconnectDelayMillis);

                    // TODO Handle connection lost. maybe we need to pause RaftPeerNode when it's connection was lost
                    scheduleReconnectToClientJob(scheduler, addr);
                });
            } else {
                logger.warn("connect to {} failed, start reconnect after {} millis", addr, this.clientReconnectDelayMillis);
                scheduleReconnectToClientJob(scheduler, addr);
            }
        });
    }

    public void connectToClients(List<String> addrs) {
        addrs.forEach(this::connect);
    }

    private Optional<Channel> getOrCreateChannel(String addr) {
        ChannelFuture channelFuture = this.channelMap.get(addr);
        if (channelFuture != null) {
            Channel ch = channelFuture.channel();
            if (ch.isActive()) {
                return Optional.of(ch);
            }

            return Optional.empty();
        } else {
            logger.info("create channel for addr %s, currently known addrs is %s", addr, this.channelMap.keySet());
            this.connect(addr);
            return Optional.empty();
        }
    }

    private Future<Void> doSend(String addr, RemotingCommand cmd) {
        final Optional<Channel> chOp = this.getOrCreateChannel(addr);
        if (chOp.isPresent()) {
            final Channel ch = chOp.get();
            logger.debug("send remoting command {} to {}", cmd, addr);
            ChannelFuture future = ch.writeAndFlush(cmd);
            future.addListener(f -> {
                if (!f.isSuccess()) {
                    logger.warn("send request to {} failed", NettyRemoteClient.this, f.cause());
                    if (!cmd.isOneWay()) {
                        this.removePendingRequest(cmd.getRequestId());
                    }
                    ch.close();
                }
            });
            return future;
        } else {
            if (!cmd.isOneWay()) {
                this.removePendingRequest(cmd.getRequestId());
            }

            return GlobalEventExecutor.INSTANCE.newFailedFuture(new RuntimeException("connection lost"));
        }
    }

    public Future<Void> send(String addr, RemotingCommand cmd, PendingRequestCallback callback) {
        this.addPendingRequest(cmd.getRequestId(), 3000, callback);
        return doSend(addr, cmd);
    }

    public Future<Void> sendOneway(String addr, RemotingCommand cmd) {
        cmd.markOneWay(true);
        return doSend(addr, cmd);
    }

    void shutdown() {
        try {
            this.timer.shutdown();
            this.callbackExecutor.shutdown();
            this.eventLoopGroup.shutdownGracefully();
        } catch (Exception ex) {
            logger.error("got exception on shutting down NettyRemoteClient");
        }
    }

    private ExecutorService getCallbackExecutor() {
        return this.callbackExecutor;
    }

    public void addPendingRequest(int requestId, long timeoutMillis, PendingRequestCallback callback) {
        PendingRequest pending = new PendingRequest(timeoutMillis, callback);
        this.pendingRequestTable.put(requestId, pending);
    }

    public void removePendingRequest(int requestId) {
        this.pendingRequestTable.remove(requestId);
    }

    private void executeRequestCallback(PendingRequest pendingRequest) {
        final ExecutorService executor = this.getCallbackExecutor();
        if (executor != null) {
            try {
                executor.submit(() -> {
                    try {
                        pendingRequest.executeCallback();
                    } catch (Exception ex) {
                        logger.error("execute pending request callback failed", ex);
                        logger.error(pendingRequest.toString());
                    }
                });
            } catch (Exception ex) {
                logger.error("callback thread pool is busy, executing request callback failed", ex);
                logger.error(pendingRequest.toString());
            }
        } else {
            logger.error("no callback executor service");
        }
    }

    private void scanPendingRequestTable() {
        final List<PendingRequest> timeoutRequest = new LinkedList<>();
        Iterator<PendingRequest> it = this.pendingRequestTable.values().iterator();
        while (it.hasNext()) {
            PendingRequest req = it.next();
            if (req.isTimeout()) {
                it.remove();
                timeoutRequest.add(req);
            }
        }

        for (final PendingRequest pr : timeoutRequest) {
            this.executeRequestCallback(pr);
        }
    }

    private void processResponseCommand(final ChannelHandlerContext ctx, final RemotingCommand res) {
        final int requestId = res.getRequestId();
        final PendingRequest pendingRequest = this.pendingRequestTable.get(requestId);
        if (pendingRequest != null) {
            this.pendingRequestTable.remove(requestId);
            pendingRequest.setResponse(res);
            this.executeRequestCallback(pendingRequest);
        } else {
            logger.warn("got response without matched pending request, maybe request have been canceled, {}",
                    Util.parseChannelRemoteAddrToString(ctx.channel()));
            logger.warn(res.toString());
        }
    }

    @ChannelHandler.Sharable
    class RemoteClientHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand req) throws Exception {
            switch (req.getType()) {
                case REQUEST:
                    logger.error("client receive request {} from {}", req, Util.parseChannelRemoteAddrToString(ctx.channel()));
                    break;
                case RESPONSE:
                    if (logger.isDebugEnabled()) {
                        logger.debug("receive response {} from {}", req, Util.parseChannelRemoteAddrToString(ctx.channel()));
                    }
                    processResponseCommand(ctx, req);
                    break;
                default:
                    logger.error("unknown remote command type {} from {}", req.toString(), Util.parseChannelRemoteAddrToString(ctx.channel()));
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("got unexpected exception on address {}", Util.parseChannelRemoteAddrToString(ctx.channel()), cause);
        }
    }
}
