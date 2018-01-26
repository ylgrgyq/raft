package raft.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.LoggerFactory;
import raft.Pair;
import raft.ThreadFactoryImpl;
import raft.Util;
import raft.server.connections.NettyDecoder;
import raft.server.connections.NettyEncoder;
import raft.server.connections.RemoteClient;
import raft.server.processor.Processor;
import raft.server.rpc.*;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 17/12/15
 */
public class RemoteServer {
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RemoteServer.class.getName());

    private final HashMap<CommandCode, Pair<Processor, ExecutorService>> processorTable = new HashMap<>();
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private ChannelHandler handler = new RemoteRequestHandler();
    private ConcurrentHashMap<Integer, PendingRequest> pendingRequestTable = new ConcurrentHashMap<>();

    private ScheduledExecutorService timer;
    private ExecutorService generalExecutor;

    RemoteServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;

        this.generalExecutor = Executors.newFixedThreadPool(4,
                new ThreadFactoryImpl("RemoteServerCallbackPoll_"));

        this.timer = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("RaftServerTimer_"));
    }

    void registerProcessor(CommandCode code, Processor processor, ExecutorService service) {
        this.processorTable.put(code, new Pair<>(processor, service));
    }

    List<RemoteClient> connectToClients(List<InetSocketAddress> addrs, long clientReconnectDelayMillis)
            throws TimeoutException, InterruptedException, ExecutionException {
        final List<RemoteClient> clients = new ArrayList<>(addrs.size());
        for (InetSocketAddress addr : addrs) {
            final RemoteClient client = new RemoteClient(this.workerGroup, this, addr);
            client.connect(clientReconnectDelayMillis);
            clients.add(client);
        }

        return clients;
    }

    ChannelFuture startLocalServer() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new NettyEncoder());
                        p.addLast(new NettyDecoder());
                        p.addLast(handler);
                    }
                });

        // Bind and start to accept incoming connections.
        ChannelFuture future = b.bind(this.port).sync();

        this.timer.scheduleWithFixedDelay(this::scanPendingRequestTable, 2, 2, TimeUnit.SECONDS);

        return future;
    }

    public void addPendingRequest(int requestId, long timeoutMillis, PendingRequestCallback callback) {
        PendingRequest pending = new PendingRequest(timeoutMillis, callback);
        this.pendingRequestTable.put(requestId, pending);
    }

    public void removePendingRequest(int requestId) {
        this.pendingRequestTable.remove(requestId);
    }

    private void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand req) {
        final Pair<Processor, ExecutorService> processorPair = processorTable.get(req.getCommandCode());
        if (processorPair != null) {
            try {
                processorPair.getRight().submit(() -> {
                    try {
                        final RemotingCommand res = processorPair.getLeft().processRequest(req);
                        if (!req.isOneWay()) {
                            res.setRequestId(req.getRequestId());
                            res.setType(RemotingCommandType.RESPONSE);
                            try {
                                logger.debug("send response " + res);
                                ctx.writeAndFlush(res);
                            } catch (Throwable e) {
                                logger.error("process done but write response failed", e);
                                logger.error(req.toString());
                                logger.error(res.toString());
                            }
                        }
                    } catch (Throwable e) {
                        logger.error("process request exception", e);
                        logger.error(req.toString());
                        // FIXME write exception and error info back
                    }
                });
            } catch (RejectedExecutionException e) {
                logger.error("too many request with command code {} and thread pool is busy, reject command from {}",
                        req.getCommandCode(), Util.parseChannelRemoteAddrToString(ctx.channel()));
            }
        } else {
            logger.error("no processor for command code {}, current supported command codes is {}",
                    req.getCommandCode(), processorTable.keySet());
        }
    }

    private ExecutorService getGeneralExecutor() {
        return this.generalExecutor;
    }

    private void executeRequestCallback(PendingRequest pendingRequest) {
        final ExecutorService executor = this.getGeneralExecutor();
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

    public ChannelHandler getHandler() {
        return handler;
    }

    void shutdown() {
        this.timer.shutdown();
        this.generalExecutor.shutdown();
        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
    }

    @ChannelHandler.Sharable
    class RemoteRequestHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand req) throws Exception {
            switch (req.getType()) {
                case REQUEST:
                    if (logger.isDebugEnabled()) {
                        logger.debug("receive request {} from {}", req, Util.parseChannelRemoteAddrToString(ctx.channel()));
                    }
                    processRequestCommand(ctx, req);
                    break;
                case RESPONSE:
                    if (logger.isDebugEnabled()) {
                        logger.debug("receive response {} from {}", req, Util.parseChannelRemoteAddrToString(ctx.channel()));
                    }
                    processResponseCommand(ctx, req);
                    break;
                default:
                    logger.warn("unknown remote command type {} from {}", req.toString(), Util.parseChannelRemoteAddrToString(ctx.channel()));
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("got unexpected exception on address {}", Util.parseChannelRemoteAddrToString(ctx.channel()), cause);
        }
    }
}
