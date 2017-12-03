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
import raft.server.rpc.AppendEntries;
import raft.server.rpc.CommandCode;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVote;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.*;

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
    private final ExecutorService processorExecutorService = Executors.newFixedThreadPool(Integer.parseInt(System.getProperty("raft.server.processor.thread.pool.size", "8")));
    private ChannelHandler handler = new RaftRequestHandler();
    private ScheduledFuture electionTimeoutFuture;


    protected RaftServer(String selfId, EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
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

//    public void transferState(RaftState newState) {
//        this.state = newState;
//    }

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }

    void startElection() {
        ThreadLocalRandom random = ThreadLocalRandom.current();
        int electionTimeoutMillis = random.nextInt(this.maxElectionTimeoutMillis);

        this.state = State.CANDIDATE;
        this.term += 1;
        List<ChannelFuture> requestVoteFutures = new ArrayList<>(this.clients.size());

        for (final RemoteRaftClient client : this.clients.values()) {
            ChannelFuture voteFuture = client.requestVote().addListener((ChannelFuture f) -> {
                if (!f.isSuccess()) {
                    logger.warn("request vote to " + client + " failed", f.cause());
                    client.close();
                }
            });
            requestVoteFutures.add(voteFuture);
        }
        this.electionTimeoutFuture = this.workerGroup.schedule(() -> {
            requestVoteFutures.forEach(f -> f.cancel(true));
            startElection();
        }, electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    Future<Void> startLocalServer() throws InterruptedException {
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

    private void registerProcessor(CommandCode code, Processor processor, ExecutorService service) {
        this.processorTable.put(code, new Pair<>(processor, service));
    }

    void registerProcessors() {
        this.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
        this.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
    }

    private void connectToClient(InetSocketAddress addr) {
        final RemoteRaftClient client = new RemoteRaftClient(this.workerGroup, this);
        final ChannelFuture future = client.connect(addr);
        future.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                logger.info("Connect to " + addr + " success");
                final Channel ch = f.channel();
                final String id = client.getId();
                clients.put(id, client);
                ch.closeFuture().addListener(cf -> {
                    logger.warn("Connection with " + addr + " lost");
                    clients.remove(id);
                    this.workerGroup.schedule(() -> connectToClient(addr),
                            clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
                });
            } else {
                logger.warn("Connect to " + addr + " failed");
                this.workerGroup.schedule(() -> connectToClient(addr),
                        clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
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
        return handler;
    }

    public int getTerm() {
        return term;
    }

    public String getId() {
        return selfId;
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

    @ChannelHandler.Sharable
    class RaftRequestHandler extends SimpleChannelInboundHandler<RemotingCommand> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, RemotingCommand o) throws Exception {
            switch (o.getCommandCode()) {
                case REQUEST_VOTE:
                    RequestVote vote = new RequestVote();
                    vote.decode(o.getBody());
                    System.out.println("Receive msg: " + vote);
                case APPEND_ENTRIES:
                    AppendEntries entry = new AppendEntries();
                    entry.decode(o.getBody());
                    System.out.println("Receive msg: " + entry);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            logger.error("got unexpected exception on address {}", Util.parseChannelRemoteAddr(ctx.channel()), cause);
        }
    }
}
