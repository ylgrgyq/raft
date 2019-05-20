package raft.server;

import raft.server.proto.LogEntry;

import java.util.List;

class ReplicateState implements PeerNodeState {
    @Override
    public String stateName() {
        return "Replicate";
    }

    @Override
    public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
        long lastEntryIndex = entriesToSend.get(entriesToSend.size() - 1).getIndex();
        node.inflights.addInflightIndex(lastEntryIndex);
        node.setNextIndex(lastEntryIndex + 1);
    }

    @Override
    public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
        node.inflights.freeTo(successIndex);
    }

    @Override
    public void onPongReceived(RaftPeerNode node) {
        if (node.inflights.isFull()) {
            node.inflights.forceFreeFirstOne();
        }
    }

    @Override
    public boolean nodeIsPaused(RaftPeerNode node) {
        return node.inflights.isFull();
    }
}