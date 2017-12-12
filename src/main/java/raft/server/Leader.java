package raft.server;

import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
public class Leader extends RaftState {
    private Logger logger = LoggerFactory.getLogger(Leader.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private RaftServer server;
    private ScheduledFuture pingTimeoutFuture;
    private long pingIntervalMillis = Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "300"));

    public void start() {
        this.pingTimeoutFuture = this.timer.scheduleWithFixedDelay(() -> {
            // ping to all clients
            }, 100, 100, TimeUnit.SECONDS);
    }

    private void schedulePingJob() {
        this.pingTimeoutFuture = this.timer.scheduleWithFixedDelay(() -> {
            logger.info("Ping to all clients...");
            for (final RemoteRaftClient client : this.server.getConnectedClients().values()) {
                client.ping().addListener((ChannelFuture f) -> {
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

    void transferToCandidate(){
        throw new UnsupportedOperationException();
    }

    void transferToLeader() {
        throw new UnsupportedOperationException();
    }

    void transferToFollower() {
        finish();
        // schedule ping timeout
    }

    public void onReceiveAppendEntries(AppendEntriesCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        }
    }

    public void onReceiveRequestVote(RequestVoteCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        } else {

        }
    }
}
