package raft.server.connections;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.slf4j.LoggerFactory;
import raft.Util;
import raft.server.RaftServer;
import raft.server.rpc.*;

import java.net.InetSocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemoteRaftClient {
    private org.slf4j.Logger logger = LoggerFactory.getLogger(RemoteRaftClient.class.getName());

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

    public void send(RemotingCommand req) {

    }

    public ChannelFuture requestVote(PendingRequestCallback callable) {
        RequestVoteCommand vote = new RequestVoteCommand();
        vote.setCandidateId(server.getId());

        RemotingCommand cmd = RemotingCommand.createRequestCommand();
        cmd.setTerm(server.getTerm());
        cmd.setCommandCode(CommandCode.APPEND_ENTRIES);
        cmd.setBody(vote.encode());

        this.server.addPendingRequest(cmd, callable);
        ChannelFuture future = channelFuture.channel().writeAndFlush(cmd);
        future.addListener(f -> {
            if (!f.isSuccess()) {
                logger.warn("request vote to {} failed", this, f.cause());
                this.server.removePendingRequest(cmd.getRequestId());
                this.close();
            }
        });
        return future;
    }

    public ChannelFuture ping() {
        AppendEntriesCommand ping = new AppendEntriesCommand();
        RemotingCommand cmd = RemotingCommand.createRequestCommand();
        cmd.setTerm(server.getTerm());
        cmd.setCommandCode(CommandCode.APPEND_ENTRIES);
        cmd.setBody(ping.encode());

        return channelFuture.channel().writeAndFlush(cmd);
    }

    public ChannelFuture close() {
        return channelFuture.channel().close();
    }

    @Override
    public String toString() {
        return "" + channelFuture.channel().remoteAddress();
    }
}
