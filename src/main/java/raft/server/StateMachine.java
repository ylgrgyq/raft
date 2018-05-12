package raft.server;

import raft.server.proto.LogEntry;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine {
    void onProposalCommitted(List<LogEntry> msgs);
    void onNodeAdded(String peerId);
    void onNodeRemoved(String peerId);
    void onShutdown();
}
