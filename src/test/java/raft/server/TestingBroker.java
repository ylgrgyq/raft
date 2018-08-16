package raft.server;

import org.slf4j.Logger;
import raft.server.proto.RaftCommand;

import java.util.Map;


class TestingBroker implements RaftCommandBroker {
    private final Map<String, Raft> nodes;
    private final Logger logger;

    TestingBroker(Map<String, Raft> nodes, Logger logger) {
        this.nodes = nodes;
        this.logger = logger;
    }

    @Override
    public void onWriteCommand(RaftCommand cmd) {
        logger.debug("node {} write command {}", cmd.getFrom(), cmd.toString());
        String to = cmd.getTo();
        Raft toNode = nodes.get(to);
        if (toNode != null) {
            toNode.receiveCommand(cmd);
        }
    }
}
