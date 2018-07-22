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
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Author: ylgrgyq
 * Date: 18/5/10
 */
public class StateMachineProxyTest {
    private final RaftLog raftLog = new RaftLogImpl(new MemoryBasedTestingStorage());

    @Test
    public void testNormalCase() throws Exception {
        String expectPeerId = "peerId1";
        final AtomicBoolean stateMachineCalledOnce = new AtomicBoolean();
        CountDownLatch listenerCalled = new CountDownLatch(1);

        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            String peerId = invocationOnMock.getArgument(0);
            assertTrue(expectPeerId.equals(peerId));
            assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
            listenerCalled.countDown();
            return null;
        }).when(mockStateMachine).onNodeAdded(anyString());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        proxy.onNodeAdded(expectPeerId);
        listenerCalled.await();
        assertTrue(stateMachineCalledOnce.get());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testThreadPoolQueueFull() throws Exception {
        CountDownLatch listenerCalled = new CountDownLatch(1);
        StateMachine mockedStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            try {
                listenerCalled.countDown();
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
                // ignore
            }
            return null;
        }).when(mockedStateMachine).onNodeAdded(anyString());

        ExecutorService pool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1), new ThreadFactoryImpl("StateMachineProxy-"));
        StateMachineProxy proxy = new StateMachineProxy(mockedStateMachine, raftLog, pool);
        proxy.onNodeAdded("peerId1");
        listenerCalled.await();
        proxy.onNodeAdded("peerId2");
        proxy.onNodeAdded("peerId2");
    }

    @Test(expected = IllegalStateException.class)
    public void testStateMachineThrowAnyException2() throws Exception {
        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            throw new RuntimeException("some expected exception");
        }).when(mockStateMachine)
                .onNodeAdded(anyString());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        for (int i = 0; i < 10; i++) {
            proxy.onNodeAdded("peerId1");
            Thread.sleep(200);
        }
    }

    @Test
    public void testOnShutdown() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean();

        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            assertTrue(shutdownCalled.compareAndSet(false, true));
            return null;
        }).when(mockStateMachine).onShutdown();

        ExecutorService pool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(), new ThreadFactoryImpl("StateMachineProxy-"));
        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog, pool);
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

        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            List<LogEntry> msgs = invocationOnMock.getArgument(0);
            assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
            assertEquals(originMsgs
                    .stream()
                    .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                    .collect(Collectors.toList()), msgs);
            return null;
        }).when(mockStateMachine).onProposalCommitted(anyList());

        RaftLog mockRaftLog = mock(RaftLog.class);
        doAnswer(invocationOnMock -> {
            int appliedTo = invocationOnMock.getArgument(0);
            assertEquals(configAtLast.getIndex(), appliedTo);
            appliedCalled.countDown();
            return null;
        }).when(mockRaftLog).appliedTo(anyInt());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, mockRaftLog);
        proxy.onProposalCommitted(originMsgs
                .stream()
                .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                .collect(Collectors.toList()), originMsgs.get(originMsgs.size() - 1).getIndex());
        appliedCalled.await();
        assertTrue(stateMachineCalledOnce.get());
    }

    @Test
    public void installSnapshot() throws Exception {
        CountDownLatch appliedCalled = new CountDownLatch(1);
        Snapshot expectSnapshot = Snapshot.newBuilder()
                .setTerm(2)
                .setIndex(6)
                .setData(ByteString.copyFrom(new byte[]{1, 2,3,4}))
                .build();

        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            Snapshot actualSnapshot = invocationOnMock.getArgument(0);
            assertEquals(expectSnapshot, actualSnapshot);
            return null;
        }).when(mockStateMachine).installSnapshot(any());

        RaftLog mockRaftLog = mock(RaftLog.class);
        doAnswer(invocationOnMock -> {
            int actualIndex = invocationOnMock.getArgument(0);
            assertEquals(expectSnapshot.getIndex(), actualIndex);
            appliedCalled.countDown();
            return null;
        }).when(mockRaftLog).snapshotApplied(anyInt());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, mockRaftLog);
        proxy.installSnapshot(expectSnapshot);
        appliedCalled.await();
    }

    @Test
    public void getRecentSnapshot() throws Exception {
        int expectIndex = ThreadLocalRandom.current().nextInt();
        Snapshot expectSnapshot = Snapshot.newBuilder()
                .setTerm(2)
                .setIndex(6)
                .setData(ByteString.copyFrom(new byte[]{1, 2,3,4}))
                .build();

        long mainThreadId = Thread.currentThread().getId();
        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            int actualIndex = invocationOnMock.getArgument(0);
            assertEquals(expectIndex, actualIndex);
            assertEquals(mainThreadId, Thread.currentThread().getId());
            return Optional.of(expectSnapshot);
        }).when(mockStateMachine).getRecentSnapshot(anyInt());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        Optional<Snapshot> actualSnapshot = proxy.getRecentSnapshot(expectIndex);
        assert(actualSnapshot.isPresent());
        assertEquals(expectSnapshot, actualSnapshot.get());
    }
}