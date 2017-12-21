package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.AppendEntriesCommand;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
class Follower extends RaftState<AppendEntriesCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Follower.class.getName());

    private final long pingTimeoutMillis = 2 * Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.millis", "2000"));

    private ScheduledFuture pingTimeoutFuture;

    Follower(RaftServer server, ScheduledExecutorService timer) {
        super(server, timer, State.FOLLOWER);
    }

    public void start() {
        logger.debug("start follower, server={}", this.server);
        this.schedulePingTimeout();
    }

    private synchronized void schedulePingTimeout() {
        if (this.pingTimeoutFuture == null) {
            try {
                this.pingTimeoutFuture = this.timer.schedule(() -> {
                    logger.info("not receiving ping for {} millis, start transit to candidate", this.pingTimeoutMillis);
                    this.server.tryTransitStateToCandidate();
                }, this.pingTimeoutMillis, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ex) {
                logger.error("schedule ping timeout failed", ex);
            }
        } else {
            logger.warn("ping timeout job already scheduled");
        }
    }

    public synchronized void finish() {
        logger.debug("finish follower, server={}", this.server);
        if (this.pingTimeoutFuture != null) {
            this.pingTimeoutFuture.cancel(true);
            this.pingTimeoutFuture = null;
        }
    }

    @Override
    public void onReceiveRaftServerCommand(AppendEntriesCommand cmd) {
        if (this.server.getState() == State.FOLLOWER) {
            if (this.pingTimeoutFuture != null) {
                synchronized (this) {
                    if (this.pingTimeoutFuture != null) {
                        this.pingTimeoutFuture.cancel(true);
                        this.pingTimeoutFuture = null;
                        this.schedulePingTimeout();
                    }
                }
            }
        }
    }
}
