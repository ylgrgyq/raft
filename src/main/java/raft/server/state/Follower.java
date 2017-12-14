package raft.server.state;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
public class Follower extends RaftState {
    private static final Logger logger = LoggerFactory.getLogger(Follower.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final long pingTimeoutMillis = 2 * Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "300"));

    private RaftServer server;
    private ScheduledFuture pingTimeoutFuture;

    public Follower(RaftServer server) {
        this.server = server;
    }

    public void start() {
        this.schedulePingTimeout();
    }

    private void schedulePingTimeout() {
        if (this.pingTimeoutFuture == null) {
            logger.warn("not receiving ping for {} millis transit to candidate", this.pingTimeoutMillis);
            this.pingTimeoutFuture = this.timer.schedule(() ->
                            this.server.transitState(RaftServer.State.CANDIDATE)
                    , this.pingTimeoutMillis, TimeUnit.SECONDS);
        } else {
            logger.warn("ping timeout job already scheduled");
        }
    }

    public void finish() {
        this.pingTimeoutFuture.cancel(true);
        this.pingTimeoutFuture = null;
    }

    public void onReceiveAppendEntries(AppendEntriesCommand cmd) {
        this.pingTimeoutFuture.cancel(true);
        this.schedulePingTimeout();
    }
}
