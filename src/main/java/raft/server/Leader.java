package raft.server;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
class Leader extends RaftState<RaftServerCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class.getName());

    private final long pingIntervalMillis = Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "2000"));

    private ScheduledFuture pingTimeoutFuture;

    Leader(RaftServer server, ScheduledExecutorService timer) {
        super(server, timer, State.LEADER);
    }

    public void start() {
        logger.debug("start leader, server={}", this.server);
        this.pingTimeoutFuture = this.schedulePingJob();
    }

    private ScheduledFuture schedulePingJob() {
        try {
            return this.timer.scheduleWithFixedDelay(() -> {
                final AppendEntriesCommand ping = new AppendEntriesCommand(server.getTerm());
                ping.setLeaderId(server.getLeaderId());
                logger.debug("ping to all clients, ping={} ...", ping);
                for (final RemoteRaftClient client : this.server.getConnectedClients().values()) {
                    RemotingCommand cmd = RemotingCommand.createRequestCommand(ping);
                    client.sendOneway(cmd).addListener((ChannelFuture f) -> {
                        if (!f.isSuccess()) {
                            logger.warn("ping to {} failed", client, f.cause());
                            client.close();
                        }
                    });
                }
            }, this.pingIntervalMillis, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            logger.error("schedule sending ping failed, will lose leadership later", ex);
        }
        return null;
    }

    public void finish() {
        logger.debug("finish leader, server={}", this.server);
        if (this.pingTimeoutFuture != null) {
            this.pingTimeoutFuture.cancel(true);
        }
    }
}