package raft.server.state;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
public class Leader extends RaftState {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final long pingIntervalMillis = Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "300"));

    private RaftServer server;
    private ScheduledFuture pingTimeoutFuture;

    public Leader(RaftServer server) {
        this.server = server;
    }

    public void start() {
        this.pingTimeoutFuture = this.schedulePingJob();
    }

    private ScheduledFuture schedulePingJob() {
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
    }

    public void finish() {
        this.pingTimeoutFuture.cancel(true);
    }
}
