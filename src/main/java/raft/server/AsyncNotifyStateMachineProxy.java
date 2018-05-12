package raft.server;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
class AsyncNotifyStateMachineProxy implements StateMachine {
    private static final Logger logger = LoggerFactory.getLogger(AsyncNotifyStateMachineProxy.class.getName());

    private final ExecutorService notifier;
    private final StateMachine stateMachine;
    private volatile boolean unexpectedStateMachineException = false;

    AsyncNotifyStateMachineProxy(StateMachine stateMachine) {
        this(stateMachine, Executors.newSingleThreadExecutor(
                new ThreadFactoryImpl("StateMachineProxy-")));
    }

    AsyncNotifyStateMachineProxy(StateMachine stateMachine, ExecutorService notifier) {
        Preconditions.checkNotNull(stateMachine);
        Preconditions.checkNotNull(notifier);

        this.notifier = notifier;
        this.stateMachine = stateMachine;
    }

    private CompletableFuture<Void> notifyStateMachine(Runnable job) {
        if (!unexpectedStateMachineException) {
            return CompletableFuture
                    .runAsync(job, notifier)
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            logger.error("notify state machine failed, will not accept any new notification job afterward", ex);
                            unexpectedStateMachineException = true;
                        }
                    });
        } else {
            throw new RuntimeException("StateMachine shutdown due to unexpected exception, please check log to debug");
        }
    }

    @Override
    public void onProposalCommitted(List<LogEntry> msgs) {
        notifyStateMachine(() -> stateMachine.onProposalCommitted(msgs));
    }

    @Override
    public void onNodeAdded(final String peerId) {
        notifyStateMachine(() -> stateMachine.onNodeAdded(peerId));
    }

    @Override
    public void onNodeRemoved(final String peerId) {
        notifyStateMachine(() -> stateMachine.onNodeRemoved(peerId));
    }

    @Override
    public void onShutdown() {
        notifyStateMachine(stateMachine::onShutdown);
    }

    void shutdown() {
        notifier.shutdown();
    }
}
