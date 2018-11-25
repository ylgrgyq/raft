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
    public void onPongReceived(RaftPeerNode node) {
        // The implementation of pipeline is referenced a lot from https://github.com/etcd-io/etcd/blob/master/raft/design.md
        // but in etcd user need to call ReportSnapshot: https://github.com/etcd-io/etcd/blob/v3.3.10/raft/node.go#L165
        // after the snapshot is sent to follower. If user failed to do that, follower will remain in a
        // limbo state, never getting any updates from leader.
        // So here we just add a timeout to handle this situation without add a extra API.
        if (node.increaseSnapshotTimeout() > 10) {
            node.pendingSnapshotIndex = 0;
            node.transferToProbe();
        }
    }

    @Override
    public boolean nodeIsPaused(RaftPeerNode node) {
        return true;
    }
}
