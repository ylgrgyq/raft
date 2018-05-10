package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
class AsyncNotifyStateMachineProxy implements StateMachine{
    private static final Logger logger = LoggerFactory.getLogger(AsyncNotifyStateMachineProxy.class.getName());

    private final ExecutorService stateMachineNotifier = Executors.newSingleThreadExecutor();
    private final StateMachine stateMachine;
    private volatile boolean unexpectedStateMachineException = false;

    AsyncNotifyStateMachineProxy(StateMachine stateMachine) {
        this.stateMachine = stateMachine;
    }

    private void notifyStateMachine(Runnable job) {
        if (!unexpectedStateMachineException) {
            try {
                stateMachineNotifier.submit(job);
            } catch (RejectedExecutionException ex) {
                throw new RuntimeException("submit apply proposal job failed", ex);
            }
        } else {
            throw new RuntimeException("StateMachine shutdown due to unexpected exception, please check log to debug");
        }
    }

    @Override
    public void onProposalCommitted(List<LogEntry> msgs) {
        notifyStateMachine(() -> {
            try {
                stateMachine.onProposalCommitted(msgs);
            } catch (Exception ex) {
                logger.error("onProposalCommitted notify failed", ex);
                unexpectedStateMachineException = true;
            }
        });
    }

    @Override
    public void onNodeAdded(String peerId) {
        notifyStateMachine(() -> {
            try {
                stateMachine.onNodeAdded(peerId);
            } catch (Exception ex) {
                logger.error("onNodeAdded notify failed", ex);
                unexpectedStateMachineException = true;
            }
        });
    }

    @Override
    public void onNodeRemoved(String peerId) {
        notifyStateMachine(() -> {
            try {
                stateMachine.onNodeRemoved(peerId);
            } catch (Exception ex) {
                logger.error("onNodeRemoved notify failed", ex);
                unexpectedStateMachineException = true;
            }
        });
    }
}
