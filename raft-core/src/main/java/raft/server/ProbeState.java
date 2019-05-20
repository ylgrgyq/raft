package raft.server;

import raft.server.proto.LogEntry;
import java.util.List;


class ProbeState implements  PeerNodeState {
    @Override
    public String stateName() {
        return "Probe";
    }

    @Override
    public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
        node.pause();
    }

    @Override
    public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
        node.transferToReplicate(successIndex);
    }

    @Override
    public boolean nodeIsPaused(RaftPeerNode node) {
        return node.pause;
    }
}
