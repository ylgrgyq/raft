package raft.server;

import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine {
    void onProposalCommitted(RaftStatusSnapshot status, List<LogEntry> msgs);
    void onNodeAdded(RaftStatusSnapshot status, String peerId);
    void onNodeRemoved(RaftStatusSnapshot status, String peerId);
    void onLeaderStart(RaftStatusSnapshot status);
    void onLeaderFinish(RaftStatusSnapshot status);
    void onFollowerStart(RaftStatusSnapshot status);
    void onFollowerFinish(RaftStatusSnapshot status);
    void onCandidateStart(RaftStatusSnapshot status);
    void onCandidateFinish(RaftStatusSnapshot status);
    void installSnapshot(RaftStatusSnapshot status, LogSnapshot snap);

    /**
     * Get recent LogSnapshot with expected index.
     *
     * Please note that this function should not block. If it does, it will block the raft main thread which may
     * cause the entire raft service malfunction.
     *
     * @param expectIndex indicate that we need a LogSnapshot which covers logs at least to this index
     * @return A raft LogSnapshot wrapped by Optional if there's already a prepared snapshot which covers the expectedIndex,
     * or else return the Optional.empty() if there is not.
     */
    Optional<LogSnapshot> getRecentSnapshot(int expectIndex);

    void onShutdown();
}
