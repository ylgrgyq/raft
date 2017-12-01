package raft.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.NettyDecoder;
import raft.server.connections.NettyEncoder;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.Request;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


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

    private long clientReconnectDelayMillis;
    private long pingIntervalMillis;
    private ChannelFuture serverChannelFuture;
    private State state;
    private int currentTerm;
    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
    private ChannelHandler handler = new RaftRequestHandler();

    protected RaftServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
                         long clientReconnectDelayMillis, long pingIntervalMillis, State state) {
        this.currentTerm = 0;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        this.clientReconnectDelayMillis = clientReconnectDelayMillis;
        this.pingIntervalMillis = pingIntervalMillis;
        this.state = state;
    }

//    public void transferState(RaftState newState) {
//        this.state = newState;
//    }

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }

    public void startElection() {

    }

    Future<Void> startLocalServer() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new LineBasedFrameDecoder(123123123));
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
                        logger.warn("Ping to " + client + " failed");
                        client.close();
                    }
                });
            }
            logger.info("Ping to all clients done");
        }, this.pingIntervalMillis, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
        return this.serverChannelFuture;
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
        return currentTerm;
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

        RaftServer build() {
            return new RaftServer(this.bossGroup, this.workerGroup, this.port,
                    this.clientReconnectDelayMillis, this.pingIntervalMillis, this.state);
        }
    }

    class RaftRequestHandler extends SimpleChannelInboundHandler<Request> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Request o) throws Exception {
            System.out.println("Receive msg: " + o);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.out.println("Got exception" + cause.getMessage());
            cause.printStackTrace();
        }
    }
}
