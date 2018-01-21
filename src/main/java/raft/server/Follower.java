package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RaftCommand;
import raft.server.rpc.RequestVoteCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
class Follower extends RaftState<RaftCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Follower.class.getName());

    Follower(RaftServer server) {
        super(server, State.FOLLOWER);
    }

    public void start() {
        logger.debug("start follower, server={}", this.server);
    }

    public synchronized void finish() {
        logger.debug("finish follower, server={}", this.server);
    }

    @Override
    public void processTickTimeout(long currentTick) {
        logger.info("election timeout, become candidate");
        try {
            this.server.tryBecomeCandidate();
        } catch (Exception ex) {
            logger.error("become candidate failed", ex);
        }
    }

    @Override
    public void onReceiveRaftCommand(RaftCommand cmd) {

    }
}
