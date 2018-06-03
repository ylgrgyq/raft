package raft.server;

import raft.server.proto.LogEntry;
import raft.server.proto.Snapshot;

import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine {
    void onProposalCommitted(List<LogEntry> msgs);
    void onNodeAdded(String peerId);
    void onNodeRemoved(String peerId);
    void onLeaderStart(int term);
    void onLeaderFinish();
    void onFollowerStart(int term, String leaderId);
    void onFollowerFinish();
    void installSnapshot(Snapshot snap);

    /**
     * Get recent snapshot with expected index.
     *
     * Please note that this function should not block. If it does, it will block the raft main thread which may
     * cause the entire raft service malfunction.
     *
     * @param expectIndex indicate that we need a Snapshot which covers logs at least to this index
     * @return A raft Snapshot wrapped by Optional if there's already a prepared snapshot which covers the expectedIndex,
     * or else return the Optional.empty() if there is not.
     */
    Optional<Snapshot> getRecentSnapshot(int expectIndex);

    void onShutdown();
}
