package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.Map;


class TestingBroker implements RaftCommandBroker {
    private final Logger logger;

    private final Map<String, Raft> nodes;
    private final ArrayList<RaftCommand> buffer = new ArrayList<>();

    TestingBroker(Map<String, Raft> nodes, Logger logger) {
        this.nodes = nodes;
        this.logger = logger;
    }

    @Override
    public void onWriteCommand(RaftCommand cmd) {
        buffer.add(cmd);
    }

    @Override
    public void onFlushCommand() {
        for (RaftCommand cmd : buffer) {
            logger.debug("node {} write command {}", cmd.getFrom(), cmd.toString());
            String to = cmd.getTo();
            Raft toNode = nodes.get(to);
            if (toNode != null) {
                toNode.receiveCommand(cmd);
            }
        }
        buffer.clear();
    }

    @Override
    public void shutdown() {
        // todo wait buffer clear then return
    }
}
