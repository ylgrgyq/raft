package raft.server;

import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
class AsyncNotifyStateMachineProxy extends AsyncProxy implements StateMachine {
    private final StateMachine stateMachine;

    AsyncNotifyStateMachineProxy(StateMachine stateMachine) {
        this.stateMachine = Objects.requireNonNull(stateMachine);
    }

    AsyncNotifyStateMachineProxy(StateMachine stateMachine, ExecutorService pool) {
        super(pool);
        this.stateMachine = Objects.requireNonNull(stateMachine);
    }

    @Override
    public void onProposalCommitted(List<LogEntry> msgs) {
        notify(() -> stateMachine.onProposalCommitted(msgs));
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
    public void onLeaderStart() {
        notify(stateMachine::onLeaderStart);
    }

    @Override
    public void onLeaderFinish() {
        notify(stateMachine::onLeaderFinish);
    }

    @Override
    public void onShutdown() {
        notify(stateMachine::onShutdown);
    }
}
