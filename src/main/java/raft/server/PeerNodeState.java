package raft.server;

import raft.server.proto.LogEntry;
import java.util.List;

interface PeerNodeState {
    String stateName();

    void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend);

    void onReceiveAppendSuccess(RaftPeerNode node, long successIndex);

    default void onPongReceived(RaftPeerNode node) {}

    boolean nodeIsPaused(RaftPeerNode node);
}
