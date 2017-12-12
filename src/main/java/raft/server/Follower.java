package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
public class Follower extends RaftState {
    private Logger logger = LoggerFactory.getLogger(Follower.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private RaftServer server;
    private ScheduledFuture pingTimeoutFuture;

    public void start() {
        this.schedulePingTimeout();
    }

    private void schedulePingTimeout() {
        this.pingTimeoutFuture = this.timer.schedule(this::transferToCandidate
                , 100, TimeUnit.SECONDS);
    }

    public void finish() {
        this.pingTimeoutFuture.cancel(true);
    }

    void transferToCandidate(){
        this.finish();
        // set server state to candidate
    }

    void transferToLeader() {
        throw new UnsupportedOperationException();
    }

    void transferToFollower() {
        this.finish();
        this.schedulePingTimeout();
    }

    public void onReceiveAppendEntries(AppendEntriesCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        } else {
            this.pingTimeoutFuture.cancel(true);
            this.schedulePingTimeout();
        }
    }

    public void onReceiveRequestVote(RequestVoteCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        } else {
            // reject vote
        }
    }
}
