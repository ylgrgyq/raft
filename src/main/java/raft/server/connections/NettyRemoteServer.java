package raft.server.connections;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Pair;
import raft.Util;
import raft.server.RemoteServer;
import raft.server.processor.Processor;
import raft.server.rpc.CommandCode;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RemotingCommandType;

import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

/**
 * Author: ylgrgyq
 * Date: 18/1/25
 */
public class NettyRemoteServer {
    private static final Logger logger = LoggerFactory.getLogger(RemoteServer.class.getName());

    private final HashMap<CommandCode, Pair<Processor, ExecutorService>> processorTable = new HashMap<>();
    private final int port;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    NettyRemoteServer(EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port) {
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.port = port;
    }

    void registerProcessor(CommandCode code, Processor processor, ExecutorService service) {
        this.processorTable.put(code, new Pair<>(processor, service));
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
                        p.addLast(new RemoteServerHandler());
                    }
                });

        // Bind and start to accept incoming connections.
        return b.bind(this.port).sync();
    }


    void shutdown() {
        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
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

    @ChannelHandler.Sharable
    class RemoteServerHandler extends SimpleChannelInboundHandler<RemotingCommand> {
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
                    logger.error("server receive response {} from {}", req, Util.parseChannelRemoteAddrToString(ctx.channel()));
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
