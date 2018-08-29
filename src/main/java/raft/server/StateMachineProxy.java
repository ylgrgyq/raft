package raft.server;

import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
class StateMachineProxy extends AsyncProxy implements StateMachine {
    private final StateMachine stateMachine;
    private final RaftLog raftLog;

    StateMachineProxy(StateMachine stateMachine, RaftLog raftLog) {
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.raftLog = Objects.requireNonNull(raftLog);
    }

    StateMachineProxy(StateMachine stateMachine, RaftLog raftLog, ExecutorService pool) {
        super(pool);
        this.stateMachine = Objects.requireNonNull(stateMachine);
        this.raftLog = Objects.requireNonNull(raftLog);
    }

    CompletableFuture<Void> onProposalCommitted(RaftStatusSnapshot status, List<LogEntry> msgs, long lastIndex) {
        return notify(() -> {
            if (! msgs.isEmpty()) {
                stateMachine.onProposalCommitted(status, msgs);
            }
            raftLog.appliedTo(lastIndex);
        });
    }

    @Override
    public void onProposalCommitted(RaftStatusSnapshot status, List<LogEntry> msgs) {
        stateMachine.onProposalCommitted(status, msgs);
    }

    @Override
    public void onNodeAdded(RaftStatusSnapshot status, final String peerId) {
        notify(() -> stateMachine.onNodeAdded(status, peerId));
    }

    @Override
    public void onNodeRemoved(RaftStatusSnapshot status, final String peerId) {
        notify(() -> stateMachine.onNodeRemoved(status, peerId));
    }

    @Override
    public void onLeaderStart(RaftStatusSnapshot status) {
        notify(() -> stateMachine.onLeaderStart(status));
    }

    @Override
    public void onLeaderFinish(RaftStatusSnapshot status) {
        notify(()->stateMachine.onLeaderFinish(status));
    }

    @Override
    public void onFollowerStart(RaftStatusSnapshot status) {
        notify(() -> stateMachine.onFollowerStart(status));
    }

    @Override
    public void onFollowerFinish(RaftStatusSnapshot status) {
        notify(()->stateMachine.onFollowerFinish(status));
    }

    @Override
    public void onCandidateStart(RaftStatusSnapshot status) {
        notify(() -> stateMachine.onCandidateStart(status));
    }

    @Override
    public void onCandidateFinish(RaftStatusSnapshot status) {
        notify(()->stateMachine.onCandidateFinish(status));
    }

    @Override
    public void installSnapshot(RaftStatusSnapshot status, LogSnapshot snap) {
        notify(() -> {
            stateMachine.installSnapshot(status, snap);
            raftLog.snapshotApplied(snap.getIndex());
        });
    }

    @Override
    public Optional<LogSnapshot> getRecentSnapshot(long expectIndex) {
        return stateMachine.getRecentSnapshot(expectIndex);
    }

    @Override
    public void onShutdown() {
        notify(stateMachine::onShutdown);
    }
}
