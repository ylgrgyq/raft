package raft.server;

import com.google.protobuf.ByteString;
import org.junit.Test;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
import raft.server.proto.LogEntry;
import raft.server.proto.Snapshot;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
public class StateMachineProxyTest {
    private final RaftLog raftLog = new RaftLogImpl();

    @Test
    public void testNormalCase() throws Exception {
        String expectPeerId = "peerId1";
        final AtomicBoolean stateMachineCalledOnce = new AtomicBoolean();
        CountDownLatch listenerCalled = new CountDownLatch(1);
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                assertTrue(expectPeerId.equals(peerId));
                assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
                listenerCalled.countDown();
            }
        };

        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog);
        proxy.onNodeAdded(expectPeerId);
        listenerCalled.await();
        assertTrue(stateMachineCalledOnce.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testThreadPoolQueueFull() throws Exception {
        CountDownLatch listenerCalled = new CountDownLatch(1);
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                try {
                    listenerCalled.countDown();
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
        listenerCalled.await();
        proxy.onNodeAdded("peerId2");
        proxy.onNodeAdded("peerId2");
    }

    @Test(expected = RuntimeException.class)
    public void testStateMachineThrowAnyException() throws Exception {
        CountDownLatch listenerCalled = new CountDownLatch(1);
        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onNodeAdded(String peerId) {
                listenerCalled.countDown();
                throw new RuntimeException("some exception");
            }
        };

        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog);
        proxy.onNodeAdded("peerId1");
        listenerCalled.await();
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

    @Test
    public void testOnProposalCommitted() throws Exception {
        AtomicInteger index = new AtomicInteger(100);
        List<LogEntry> originMsgs = TestUtil.newDataList(10)
                .stream()
                .map(bs ->
                        LogEntry.newBuilder()
                                .setTerm(1)
                                .setIndex(index.getAndIncrement())
                                .setData(ByteString.copyFrom(bs))
                                .build())
                .collect(Collectors.toList());

        LogEntry configInMsgs = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(index.getAndIncrement())
                .setType(LogEntry.EntryType.CONFIG)
                .setData(ByteString.copyFrom(new byte[]{123}))
                .build();

        LogEntry configAtLast = LogEntry.newBuilder()
                .setTerm(1)
                .setIndex(index.getAndIncrement())
                .setType(LogEntry.EntryType.CONFIG)
                .setData(ByteString.copyFrom(new byte[]{121}))
                .build();

        originMsgs.set(ThreadLocalRandom.current().nextInt(0, originMsgs.size()), configInMsgs);
        originMsgs.add(configAtLast);

        CountDownLatch appliedCalled = new CountDownLatch(1);
        final AtomicBoolean stateMachineCalledOnce = new AtomicBoolean();

        StateMachine stateMachine = new AbstractTestingStateMachine() {
            @Override
            public void onProposalCommitted(List<LogEntry> msgs) {
                assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
                assertEquals(originMsgs
                        .stream()
                        .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                        .collect(Collectors.toList()), msgs);
            }
        };

        RaftLog raftLog = new AbstractTestingRaftLog() {
            @Override
            public List<LogEntry> getEntriesNeedToApply() {
                return originMsgs;
            }

            @Override
            public void appliedTo(int appliedTo) {
                assertEquals(configAtLast.getIndex(), appliedTo);
                appliedCalled.countDown();
            }
        };

        StateMachineProxy proxy = new StateMachineProxy(stateMachine, raftLog);
        proxy.onProposalCommitted(originMsgs
                .stream()
                .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                .collect(Collectors.toList()), originMsgs.get(originMsgs.size() - 1).getIndex());
        appliedCalled.await();
        assertTrue(stateMachineCalledOnce.get());
    }

    static abstract class AbstractTestingRaftLog implements RaftLog {
        @Override
        public void init() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getLastIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<Integer> getTerm(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<LogEntry> getEntry(int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<LogEntry> getEntries(int start, int end) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletableFuture<Integer> leaderAsyncAppend(int term, List<LogEntry> entries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int followerSyncAppend(int prevIndex, int prevTerm, List<LogEntry> entries) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isUpToDate(int term, int index) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getCommitIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getAppliedIndex() {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<LogEntry> tryCommitTo(int commitTo) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<LogEntry> getEntriesNeedToApply() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void appliedTo(int appliedTo) {
            throw new UnsupportedOperationException();
        }
    }

    static abstract class AbstractTestingStateMachine implements StateMachine {
        @Override
        public void onProposalCommitted(List<LogEntry> msgs) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onLeaderStart(int term) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onLeaderFinish() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onFollowerStart(int term, String leaderId) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onFollowerFinish() {
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

        @Override
        public void saveSnapshot(Snapshot snap) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Snapshot generateSnapshot() {
            throw new UnsupportedOperationException();
        }
    }

}