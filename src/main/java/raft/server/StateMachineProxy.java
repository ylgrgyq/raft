package raft.server;

import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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

    void onProposalCommitted() {
        notify(() -> {
            final List<LogEntry> msgs = raftLog.getEntriesNeedToApply();
            final List<LogEntry> msgsWithoutConfigLog = msgs
                    .stream()
                    .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                    .collect(Collectors.toList());
            if (! msgsWithoutConfigLog.isEmpty()) {
                stateMachine.onProposalCommitted(msgsWithoutConfigLog);
            }
            LogEntry lastE = msgs.get(msgs.size() - 1);
            raftLog.appliedTo(lastE.getIndex());
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
