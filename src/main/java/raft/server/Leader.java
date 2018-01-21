package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.RaftServerCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
class Leader extends RaftState<RaftServerCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Leader.class.getName());

    private final long pingIntervalTicks = Integer.parseInt(System.getProperty("raft.server.leader.ping.interval.ticks", "20"));

    Leader(RaftServer server) {
        super(server, State.LEADER);
    }

    public void start() {
        logger.debug("start leader, server={}", this.server);
        this.server.broadcastAppendEntries();
    }

    public void finish() {
        logger.debug("finish leader, server={}", this.server);
    }

    @Override
    public void processTickTimeout(long currentTick) {
        if (currentTick >= this.pingIntervalTicks) {
            this.server.broadcastAppendEntries();
        }
    }
}
