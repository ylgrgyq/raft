package raft.server;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
class Leader extends RaftState {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class.getName());

    private final long pingIntervalMillis = Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "300"));

    private ScheduledFuture pingTimeoutFuture;

    Leader(RaftServer server, ScheduledExecutorService timer) {
        super(server, timer, State.LEADER);
    }

    public void start() {
        this.pingTimeoutFuture = this.schedulePingJob();
    }

    private ScheduledFuture schedulePingJob() {
        try {
            return this.timer.scheduleWithFixedDelay(() -> {
                logger.info("Ping to all clients...");
                final AppendEntriesCommand ping = new AppendEntriesCommand(server.getTerm());
                RemotingCommand cmd = RemotingCommand.createRequestCommand(ping);
                ping.setLeaderId(server.getLeaderId());
                for (final RemoteRaftClient client : this.server.getConnectedClients().values()) {
                    client.send(cmd, null).addListener((ChannelFuture f) -> {
                        if (!f.isSuccess()) {
                            logger.warn("Ping to {} failed", client, f.cause());
                            client.close();
                        }
                    });
                }
                logger.info("Ping to all clients done");
            }, this.pingIntervalMillis, this.pingIntervalMillis, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException ex) {
            logger.error("schedule sending ping failed, will lose leadership later", ex);
        }
        return null;
    }

    public void finish() {
        if (this.pingTimeoutFuture != null) {
            this.pingTimeoutFuture.cancel(true);
        }
    }
}
