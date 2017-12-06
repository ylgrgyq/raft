package raft.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Pair;
import raft.Util;
import raft.server.connections.NettyDecoder;
import raft.server.connections.NettyEncoder;
import raft.server.connections.RemoteRaftClient;
import raft.server.processor.AppendEntriesProcessor;
import raft.server.processor.Processor;
import raft.server.processor.RequestVoteProcessor;
import raft.server.rpc.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer {
    private Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ConcurrentHashMap<String, RemoteRaftClient> clients = new ConcurrentHashMap<>();
    private final int maxElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.max.election.timeout.millis", "300"));
    private final HashMap<CommandCode, Pair<Processor, ExecutorService>> processorTable = new HashMap<>();

    private long clientReconnectDelayMillis;
    private long pingIntervalMillis;
    private ChannelFuture serverChannelFuture;
    private State state;
    private int term;
    private String selfId;
    private String leaderId;
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService processorExecutorService = Executors.newFixedThreadPool(Integer.parseInt(System.getProperty("raft.server.processor.thread.pool.size", "8")));
    private ChannelHandler handler = new RaftRequestHandler();
    private ScheduledFuture electionTimeoutFuture;
    private ConcurrentHashMap<Integer, PendingRequest> pendingRequestTable = new ConcurrentHashMap<>();



    private RaftServer(String selfId, EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
                         long clientReconnectDelayMillis, long pingIntervalMillis, State state) {
        this.term = 0;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        this.clientReconnectDelayMillis = clientReconnectDelayMillis;
        this.pingIntervalMillis = pingIntervalMillis;
        this.state = state;
        this.selfId = selfId;
    }

    public synchronized void transferStateToFollower(String leaderId) {
        this.state = State.FOLLOWER;
        this.leaderId = leaderId;
    }

    private void scheduleElectionJob() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int electionTimeoutMillis = random.nextInt(this.maxElectionTimeoutMillis);

        this.electionTimeoutFuture = this.workerGroup.schedule(this::startElection
                , electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    synchronized void startElection() {
        if (this.state != State.LEADER) {
            this.state = State.CANDIDATE;
            this.term += 1;

            final int clientsSize = this.clients.size();
            final List<ChannelFuture> requestVoteFutures = new ArrayList<>(clientsSize);
            final int votesNeedToWinLeader = clientsSize / 2;

            final AtomicInteger votesGot = new AtomicInteger();
            for (final RemoteRaftClient client : this.clients.values()) {
                ChannelFuture voteFuture = client.requestVote(req -> {
                    RequestVoteCommand v = new RequestVoteCommand();
                    v.decode(req.getResponse().getBody());
                    if (v.isVoteGranted()) {
                        if (votesGot.incrementAndGet() > votesNeedToWinLeader) {
                            this.state = State.LEADER;
                            this.electionTimeoutFuture.cancel(true);
                            this.schedulePingJob();
                        }
                    }
                });
                requestVoteFutures.add(voteFuture);
            }

            ThreadLocalRandom random = ThreadLocalRandom.current();
            int electionTimeoutMillis = random.nextInt(this.maxElectionTimeoutMillis);

            this.electionTimeoutFuture = this.workerGroup.schedule(() -> {
                requestVoteFutures.forEach(f -> f.cancel(true));
                startElection();
            }, electionTimeoutMillis, TimeUnit.MILLISECONDS);
        }
    }

    private void schedulePingJob() {
        this.workerGroup.scheduleWithFixedDelay(() -> {
            logger.info("Ping to all clients...");
            for (final RemoteRaftClient client : this.clients.values()) {
                client.ping().addListener((ChannelFuture f) -> {
                    if (!f.isSuccess()) {
                        logger.warn("Ping to " + client + " failed", f.cause());
                        client.close();
                    }
                });
            }
            logger.info("Ping to all clients done");
        }, this.pingIntervalMillis, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
    }

    private void registerProcessor(CommandCode code, Processor processor, ExecutorService service) {
        this.processorTable.put(code, new Pair<>(processor, service));
    }

    private void registerProcessors() {
        this.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
        this.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
    }

    private Future<Void> startLocalServer() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
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
        this.serverChannelFuture = b.bind(this.port).sync();
        this.workerGroup.scheduleWithFixedDelay(() -> {
            logger.info("Ping to all clients...");
            for (final RemoteRaftClient client : this.clients.values()) {
                client.ping().addListener((ChannelFuture f) -> {
                    if (!f.isSuccess()) {
                        logger.warn("Ping to " + client + " failed", f.cause());
                        client.close();
                    }
                });
            }
            logger.info("Ping to all clients done");
        }, this.pingIntervalMillis, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
        return this.serverChannelFuture;
    }

    private void scheduleReconnectToClientJob(final InetSocketAddress addr) {
        this.workerGroup.schedule(() -> this.connectToClient(addr),
                this.clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
    }

    private void connectToClient(final InetSocketAddress addr) {
        final RemoteRaftClient client = new RemoteRaftClient(this.workerGroup, this);
        final ChannelFuture future = client.connect(addr);
        future.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                logger.info("Connect to {} success", addr);
                final Channel ch = f.channel();
                final String id = client.getId();
                this.clients.put(id, client);
                ch.closeFuture().addListener(cf -> {
                    logger.warn("Connection with {} lost, start reconnect after {} millis", addr, this.clientReconnectDelayMillis);
                    this.clients.remove(id);
                    scheduleReconnectToClientJob(addr);
                });
            } else {
                logger.warn("Connect to {} failed, start reconnect after {} millis", addr, this.clientReconnectDelayMillis);
                scheduleReconnectToClientJob(addr);
            }
        });
    }

    void connectToClients(List<InetSocketAddress> addrs) {
        addrs.forEach(this::connectToClient);
    }

    void sync() throws InterruptedException {
        this.serverChannelFuture.channel().closeFuture().sync();
    }

    public ChannelHandler getHandler() {
        return this.handler;
    }

    public int getTerm() {
        return this.term;
    }

    public String getId() {
        return this.selfId;
    }

    public ScheduledFuture getElectionTimeoutFuture() {
        return this.electionTimeoutFuture;
    }

    private void processRequestCommand(final ChannelHandlerContext ctx, final RemotingCommand req) {
        final Pair<Processor, ExecutorService> processorPair = processorTable.get(req.getCommandCode());
        if (processorPair != null) {
            try {
                processorPair.getRight().submit(() -> {
                    try {
                        final RemotingCommand res = processorPair.getLeft().processRequest(req);
                        res.setRequestId(req.getRequestId());
                        res.setType(RemotingCommandType.RESPONSE);
                        try {
                            ctx.writeAndFlush(res);
                        } catch (Throwable e) {
                            logger.error("process done but write response failed", e);
                            logger.error(req.toString());
                            logger.error(res.toString());
                        }
                    } catch (Throwable e) {
                        logger.error("process request exception", e);
                        logger.error(req.toString());
                        // FIXME write exception and error info back
                    }
                });
            } catch (RejectedExecutionException e) {
                logger.error("too many request with command code {} and thread pool is busy, reject command from {}",
                        req.getCommandCode(), Util.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            logger.error("no processor for command code {}, current supported command codes is {}",
                    req.getCommandCode(), processorTable.keySet());
        }
    }

    private ExecutorService getCallbackExecutor() {
        return this.processorExecutorService;
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
            logger.warn("got response without matched pending request, {}", Util.parseChannelRemoteAddr(ctx.channel()));
            logger.warn(res.toString());
        }
    }

    void initialize() throws Exception {
        this.registerProcessors();
        this.startLocalServer();
        timer.scheduleWithFixedDelay(this::scanPendingRequestTable, 2, 2, TimeUnit.SECONDS);
    }

    public void addPendingRequest(RemotingCommand req, long timeoutMillis, PendingRequestCallback callback) {
        PendingRequest pending = new PendingRequest(req, timeoutMillis, callback);
        this.pendingRequestTable.put(req.getRequestId(), pending);
    }

    public void removePendingRequest(int requestId) {
        this.pendingRequestTable.remove(requestId);
    }

    @ChannelHandler.Sharable
    class RaftRequestHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand req) throws Exception {
            switch (req.getType()) {
                case REQUEST:
                    processRequestCommand(ctx, req);
                    break;
                case RESPONSE:
                    processResponseCommand(ctx, req);
                    break;
                default:
                    logger.warn("unknown remote command type {}", req.toString());
                    break;
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("got unexpected exception on address {}", Util.parseChannelRemoteAddr(ctx.channel()), cause);
        }
    }

    public String getLeaderId() {
        return leaderId;
    }

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    static class RaftServerBuilder {
        private long clientReconnectDelayMillis = 3000;

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private int port;
        private long pingIntervalMillis = 10000;
        private State state = State.FOLLOWER;

        RaftServerBuilder withBossGroup(EventLoopGroup group) {
            this.bossGroup = group;
            return this;
        }

        RaftServerBuilder withWorkerGroup(EventLoopGroup group) {
            this.workerGroup = group;
            return this;
        }

        RaftServerBuilder withServerPort(int port) {
            this.port = port;
            return this;
        }

        RaftServerBuilder withRole(String role) {
            this.state = State.valueOf(role);
            return this;
        }

        RaftServerBuilder withClientReconnectDelay(long delay, TimeUnit timeUnit) {
            this.clientReconnectDelayMillis = timeUnit.toMillis(delay);
            return this;
        }

        RaftServerBuilder withPingInterval(long pingInterval, TimeUnit timeUnit) {
            this.pingIntervalMillis = timeUnit.toMillis(pingInterval);
            return this;
        }

        RaftServer build() throws UnknownHostException {
            String selfId = InetAddress.getLocalHost().getHostAddress() + ":" + this.port;
            return new RaftServer(selfId, this.bossGroup, this.workerGroup, this.port,
                    this.clientReconnectDelayMillis, this.pingIntervalMillis, this.state);
        }
    }
}
