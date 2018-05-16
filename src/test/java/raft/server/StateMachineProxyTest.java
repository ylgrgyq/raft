package raft.server;

import org.junit.Test;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
public class StateMachineProxyTest {
    private final RaftLog raftLog = new RaftLog();

    @Test
    public void testNormalCase() throws Exception {
        String expectPeerId = "peerId1";
        final AtomicBoolean stateMachineCalled = new AtomicBoolean();
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                assertTrue(expectPeerId.equals(peerId));
                assertTrue(stateMachineCalled.compareAndSet(false, true));
            }
        };

        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog);
        proxy.onNodeAdded(expectPeerId);
        assertTrue(stateMachineCalled.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testThreadPoolQueueFull() throws Exception {
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    // ignore
                }
            }
        };

        ExecutorService pool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1), new ThreadFactoryImpl("StateMachineProxy-"));
        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog, pool);
        proxy.onNodeAdded("peerId1");
        proxy.onNodeAdded("peerId2");
        proxy.onNodeAdded("peerId2");
    }

    @Test(expected = RuntimeException.class)
    public void testStateMachineThrowAnyException() throws Exception {
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                throw new RuntimeException("some exception");
            }
        };

        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog);
        proxy.onNodeAdded("peerId1");
        proxy.onNodeAdded("peerId2");
    }

    @Test
    public void testOnShutdown() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean();
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onShutdown() {
                assertTrue(shutdownCalled.compareAndSet(false, true));
            }
        };

        ExecutorService pool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryImpl("StateMachineProxy-"));
        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog, pool);
        proxy.onShutdown();
        proxy.shutdown();
        assertTrue(pool.awaitTermination(2000, TimeUnit.SECONDS));
        assertTrue(shutdownCalled.get());
    }

    static abstract class AbstractTestingStateMachine implements StateMachine {
        @Override
        public void onProposalCommitted(List<LogEntry> msgs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onLeaderStart() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onLeaderFinish() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onNodeAdded(String peerId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onNodeRemoved(String peerId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onShutdown() {
            throw new UnsupportedOperationException();
        }
    }

}