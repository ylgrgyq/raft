package raft.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import raft.server.connections.RaftRequestHandler;
import raft.server.connections.RemoteRaftClient;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer {
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;
    private final ConcurrentHashMap<ChannelId, RemoteRaftClient> clients = new ConcurrentHashMap<>();

    private long clientReconnectDelayMillis;
    private long pingIntervalMillis;
    private ChannelFuture serverChannelFuture;
    private RaftState state;
    private long currentTerm;

    protected RaftServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
                         long clientReconnectDelayMillis, long pingIntervalMillis) {
        this.state = new FollowerState();
        this.currentTerm = 0;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;
        this.clientReconnectDelayMillis = clientReconnectDelayMillis;
        this.pingIntervalMillis = pingIntervalMillis;
    }

    public void transferState(RaftState newState) {
        this.state = newState;
    }

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER;
    }

    public void startElection() {

    }

    public void start() {
        RemoteRaftClient client = new RemoteRaftClient(null);
        Future<Void> channelFuture = client.connect(null);
        channelFuture.addListener(new GenericFutureListener<Future<Void>>() {
            @Override
            public void operationComplete(Future<Void> future) throws Exception {
                RaftServer.this.state.start();
            }
        });
    }

    private Future<Void> startLocalServer() throws InterruptedException {
        ServerBootstrap b = new ServerBootstrap();
        b.group(this.bossGroup, this.workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel ch) {
                        ChannelPipeline p = ch.pipeline();

                        p.addLast(new RaftRequestHandler(RaftServer.this));
                    }
                });

        // Bind and start to accept incoming connections.
        this.serverChannelFuture = b.bind(this.port).sync();
        this.workerGroup.scheduleAtFixedRate(() -> {
            for (final RemoteRaftClient client : this.clients.values()) {
                client.ping().addListener((ChannelFuture f) -> {
                    if (!f.isSuccess()) {
                        client.close();
                    }
                });
            }
        }, 0, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
        return this.serverChannelFuture;
    }

    private void connectToClient(InetSocketAddress addr) {
        RemoteRaftClient client = new RemoteRaftClient(this.workerGroup);
        ChannelFuture future = client.connect(addr);
        future.addListener((ChannelFuture f) -> {
            if (f.isSuccess()) {
                Channel ch = f.channel();
                clients.put(ch.id(), client);
                ch.closeFuture().addListener(cf -> {
                    clients.remove(ch.id());
                    this.workerGroup.scheduleWithFixedDelay(() -> connectToClient(addr),
                            0, clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
                });
            } else {
                this.workerGroup.scheduleWithFixedDelay(() -> connectToClient(addr),
                        0, clientReconnectDelayMillis, TimeUnit.MILLISECONDS);
            }
        });
    }

    private void connectToClients(List<InetSocketAddress> addrs) {
        addrs.forEach(this::connectToClient);
    }

    public void sync() throws InterruptedException {
        this.serverChannelFuture.channel().closeFuture().sync();
    }

    private static class RaftServerBuilder {
        private long clientReconnectDelayMillis = 3000;

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private int port;
        private long pingIntervalMillis = 60000;

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
                    this.clientReconnectDelayMillis, this.pingIntervalMillis);
        }
    }

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("p", "port", true, "server port");
        options.addOption("c", "config-file", true, "config properties file path");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        int serverPort = Integer.parseInt(cmd.getOptionValue("p", "6666"));
        String propertiesFile = cmd.getOptionValue("c");

        Properties prop = new Properties();
        InputStream input = null;
        try {
            if (propertiesFile == null) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                input = classLoader.getResourceAsStream("config.properties");
            } else {
                input = new FileInputStream(propertiesFile);
            }
            prop.load(input);
        } finally {
            if (input != null) {
                input.close();
            }
        }

        List<InetSocketAddress> clientAddrs = Arrays.stream(prop.getProperty("client.addrs")
                .split(","))
                .map(x -> {
                    String[] addrs = x.split(":");
                    int p = Integer.parseInt(addrs[1]);
                    return new InetSocketAddress(addrs[0], p);
                }).collect(Collectors.toList());

        System.out.println(prop.getProperty("client.addrs"));

        RaftServerBuilder serverBuilder = new RaftServerBuilder();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        serverBuilder.withBossGroup(bossGroup);
        serverBuilder.withWorkerGroup(workerGroup);
        serverBuilder.withServerPort(serverPort);

        RaftServer server = serverBuilder.build();
        try {
            server.startLocalServer();
            server.connectToClients(clientAddrs);
            server.sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }
}
