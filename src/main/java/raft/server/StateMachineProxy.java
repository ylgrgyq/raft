package raft.server;

import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.Snapshot;

import java.util.List;
import java.util.Objects;
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

    void onProposalCommitted(List<LogEntry> msgs, int lastIndex) {
        notify(() -> {
            if (! msgs.isEmpty()) {
                stateMachine.onProposalCommitted(msgs);
            }
            raftLog.appliedTo(lastIndex);
        });
    }

    @Override
    public void onProposalCommitted(List<LogEntry> msgs) {
        stateMachine.onProposalCommitted(msgs);
    }

    @Override
    public void onNodeAdded(final String peerId) {
        notify(() -> stateMachine.onNodeAdded(peerId));
    }

    @Override
    public void onNodeRemoved(final String peerId) {
        notify(() -> stateMachine.onNodeRemoved(peerId));
    }

    @Override
    public void onLeaderStart(int term) {
        notify(() -> stateMachine.onLeaderStart(term));
    }

    @Override
    public void onLeaderFinish() {
        notify(stateMachine::onLeaderFinish);
    }

    @Override
    public void onFollowerStart(int term, String leaderId) {
        notify(() -> stateMachine.onFollowerStart(term, leaderId));
    }

    @Override
    public void saveSnapshot(Snapshot snap) {
        stateMachine.saveSnapshot(snap);
    }

    @Override
    public Snapshot generateSnapshot() {
        return stateMachine.generateSnapshot();
    }

    @Override
    public void onFollowerFinish() {
        notify(stateMachine::onFollowerFinish);
    }

    @Override
    public void onShutdown() {
        notify(stateMachine::onShutdown);
    }
}
