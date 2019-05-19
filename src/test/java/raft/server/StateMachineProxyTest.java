package raft.server;

import com.google.protobuf.ByteString;
import org.junit.Test;
import raft.server.util.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
        RaftStatusSnapshot status = RaftStatusSnapshot.emptyStatus;
        doAnswer(invocationOnMock -> {
            assertEquals(status, invocationOnMock.getArgument(0));
            String peerId = invocationOnMock.getArgument(1);
            assertEquals(expectPeerId, peerId);
            assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
            listenerCalled.countDown();
            return null;
        }).when(mockStateMachine).onNodeAdded(any(), anyString());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        proxy.onNodeAdded(status, expectPeerId);
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
        }).when(mockedStateMachine).onNodeAdded(any(), anyString());

        RaftStatusSnapshot status = RaftStatusSnapshot.emptyStatus;
        ExecutorService pool = new ThreadPoolExecutor(
                1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(1), new ThreadFactoryImpl("StateMachineProxy-"));
        StateMachineProxy proxy = new StateMachineProxy(mockedStateMachine, raftLog, pool);
        proxy.onNodeAdded(status, "peerId1");
        listenerCalled.await();
        proxy.onNodeAdded(status,"peerId2");
        proxy.onNodeAdded(status,"peerId2");
    }

    @Test(expected = IllegalStateException.class)
    public void testStateMachineThrowAnyException2() throws Exception {
        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            throw new RuntimeException("some expected exception");
        }).when(mockStateMachine)
                .onNodeAdded(any(), anyString());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        for (int i = 0; i < 10; i++) {
            proxy.onNodeAdded(RaftStatusSnapshot.emptyStatus, "peerId1");
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
        proxy.shutdown().get();
        assertTrue(pool.awaitTermination(2000, TimeUnit.SECONDS));
        assertTrue(shutdownCalled.get());
    }

    @Test
    public void testOnProposalCommitted() throws Exception {
        AtomicLong index = new AtomicLong(100);
        List<LogEntry> originMsgs = TestUtil.newDataList(10)
                .stream()
                .map(bs ->
                        LogEntry.newBuilder()
                                .setTerm(1L)
                                .setIndex(index.getAndIncrement())
                                .setData(ByteString.copyFrom(bs))
                                .build())
                .collect(Collectors.toList());

        LogEntry configInMsgs = LogEntry.newBuilder()
                .setTerm(1L)
                .setIndex(index.getAndIncrement())
                .setType(LogEntry.EntryType.CONFIG)
                .setData(ByteString.copyFrom(new byte[]{123}))
                .build();

        LogEntry configAtLast = LogEntry.newBuilder()
                .setTerm(1L)
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
            assertEquals(RaftStatusSnapshot.emptyStatus, invocationOnMock.getArgument(0));
            List<LogEntry> msgs = invocationOnMock.getArgument(1);
            assertTrue(stateMachineCalledOnce.compareAndSet(false, true));
            assertEquals(originMsgs
                    .stream()
                    .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                    .collect(Collectors.toList()), msgs);
            return null;
        }).when(mockStateMachine).onProposalCommitted(any(), anyList());

        RaftLog mockRaftLog = mock(RaftLog.class);
        doAnswer(invocationOnMock -> {
            long appliedTo = invocationOnMock.getArgument(0);
            assertEquals(configAtLast.getIndex(), appliedTo);
            appliedCalled.countDown();
            return null;
        }).when(mockRaftLog).appliedTo(anyLong());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, mockRaftLog);
        proxy.onProposalCommitted(RaftStatusSnapshot.emptyStatus, originMsgs
                .stream()
                .filter(e -> (e.getType() != LogEntry.EntryType.CONFIG))
                .collect(Collectors.toList()), originMsgs.get(originMsgs.size() - 1).getIndex());
        appliedCalled.await();
        assertTrue(stateMachineCalledOnce.get());
    }

    @Test
    public void installSnapshot() throws Exception {
        CountDownLatch appliedCalled = new CountDownLatch(1);
        LogSnapshot expectSnapshot = LogSnapshot.newBuilder()
                .setTerm(2L)
                .setIndex(6L)
                .setData(ByteString.copyFrom(new byte[]{1, 2,3,4}))
                .build();

        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            assertEquals(RaftStatusSnapshot.emptyStatus, invocationOnMock.getArgument(0));
            LogSnapshot actualSnapshot = invocationOnMock.getArgument(1);
            assertEquals(expectSnapshot, actualSnapshot);
            return null;
        }).when(mockStateMachine).installSnapshot(any(), any());

        RaftLog mockRaftLog = mock(RaftLog.class);
        doAnswer(invocationOnMock -> {
            long actualIndex = invocationOnMock.getArgument(0);
            assertEquals(expectSnapshot.getIndex(), actualIndex);
            appliedCalled.countDown();
            return null;
        }).when(mockRaftLog).snapshotApplied(anyLong());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, mockRaftLog);
        proxy.installSnapshot(RaftStatusSnapshot.emptyStatus, expectSnapshot);
        appliedCalled.await();
    }

    @Test
    public void getRecentSnapshot() throws Exception {
        long expectIndex = ThreadLocalRandom.current().nextLong();
        LogSnapshot expectSnapshot = LogSnapshot.newBuilder()
                .setTerm(2L)
                .setIndex(6L)
                .setData(ByteString.copyFrom(new byte[]{1, 2,3,4}))
                .build();

        long mainThreadId = Thread.currentThread().getId();
        StateMachine mockStateMachine = mock(StateMachine.class);
        doAnswer(invocationOnMock -> {
            long actualIndex = invocationOnMock.getArgument(0);
            assertEquals(expectIndex, actualIndex);
            assertEquals(mainThreadId, Thread.currentThread().getId());
            return Optional.of(expectSnapshot);
        }).when(mockStateMachine).getRecentSnapshot(anyLong());

        StateMachineProxy proxy = new StateMachineProxy(mockStateMachine, raftLog);
        Optional<LogSnapshot> actualSnapshot = proxy.getRecentSnapshot(expectIndex);
        assert(actualSnapshot.isPresent());
        assertEquals(expectSnapshot, actualSnapshot.get());
    }
}