package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import java.util.List;

class SnapshotState implements PeerNodeState {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeerNode.class.getName());

    @Override
    public String stateName() {
        return "Snapshot";
    }

    @Override
    public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
        assert false;
        logger.error("send append msg to peer: {} on snapshot state", node);
    }

    @Override
    public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
        if (successIndex >= node.pendingSnapshotIndex) {
            node.transferToProbe();
        }
    }

    @Override
    public boolean nodeIsPaused(RaftPeerNode node) {
        return true;
    }
}
