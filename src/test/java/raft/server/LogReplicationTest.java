package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class LogReplicationTest {
    private TestingRaftCluster cluster;

    @Before
    public void before() {
        cluster = new TestingRaftCluster(LogReplicationTest.class.getSimpleName());
        cluster.clearLogStorage();
        cluster.clearPersistentState();
    }

    @After
    public void tearDown() {
        cluster.shutdownCluster();
    }

    private static void checkAppliedLogs(TestingRaftStateMachine stateMachine, int expectTerm, int logCount, List<byte[]> sourceDataList) {
        List<LogEntry> applied = stateMachine.waitApplied(logCount);

        // check node status after logs proposed
        RaftStatusSnapshot status = stateMachine.getLastStatus();
        assertEquals(logCount - 1, status.getCommitIndex());
        assertTrue(applied.size() > 0);

        for (LogEntry e : applied) {
            if (!e.equals(PersistentStorage.sentinel)) {
                assertEquals(expectTerm, e.getTerm());
                assertArrayEquals(sourceDataList.get(e.getIndex() - 1), e.getData().toByteArray());
            }
        }
    }

    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        cluster.startCluster(peers);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        // propose some logs
        List<byte[]> dataList = TestUtil.newDataList(100, 100);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = leader.propose(batch);
            ProposalResponse p = resp.get();
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }

        // check raft status after logs have been processed
        RaftStatusSnapshot status = leaderStateMachine.getLastStatus();
        assertEquals(State.LEADER, status.getState());
        assertEquals(dataList.size(), status.getCommitIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());

        List<LogEntry> applied = leaderStateMachine.drainAvailableApplied();
        for (LogEntry e : applied) {
            if (e != PersistentStorage.sentinel) {
                assertEquals(status.getTerm(), e.getTerm());
                assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
            }
        }
    }

    @Test
    public void testProposeOnTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        // propose some logs
        List<byte[]> dataList = TestUtil.newDataList(100, 100);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = leader.propose(batch);
            ProposalResponse p = resp.get();
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }

        for (TestingRaftStateMachine follower : cluster.getAllStateMachines()) {
            checkAppliedLogs(follower, follower.getLastStatus().getTerm(), 101, dataList);
        }
    }

    @Test
    public void testSyncLogOnRebootNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();

        String oldLeaderId = leaderStateMachine.getId();
        cluster.shutdownPeer(oldLeaderId);
        assertEquals(2, cluster.getAllStateMachines().size());

        leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        // propose some logs
        List<byte[]> dataList = TestUtil.newDataList(100, 100);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = leader.propose(batch);
            ProposalResponse p = resp.get(5000, TimeUnit.SECONDS);
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }

        int expectTerm = leaderStateMachine.getLastStatus().getTerm();
        for (TestingRaftStateMachine follower : cluster.getAllStateMachines()) {
            checkAppliedLogs(follower, follower.getLastStatus().getTerm(), 101, dataList);
        }

        // reboot cluster including the old leader
        cluster.shutdownCluster();
        cluster.startCluster(peerIdSet);
        leaderStateMachine = cluster.waitGetLeader();
        // old leader do not have the latest logs so it can not be elected as leader
        assertNotEquals(oldLeaderId, leaderStateMachine.getId());

        // logs should sync to old leader
        TestingRaftStateMachine follower = cluster.getStateMachineById(oldLeaderId);
        Future f = follower.becomeFollowerFuture();
        f.get();
        checkAppliedLogs(follower, expectTerm, 101, dataList);
    }

    @Test
    public void testOverwriteConflictLogs() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("overwrite conflict 001");
        peerIdSet.add("overwrite conflict 002");
        peerIdSet.add("overwrite conflict 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine oldLeaderStateMachine = cluster.waitGetLeader();
        Raft oldLeader = cluster.getNodeById(oldLeaderStateMachine.getId());

        List<TestingRaftStateMachine> followerStateMachines = cluster.getFollowers();
        followerStateMachines.forEach(s -> cluster.shutdownPeer(s.getId()));

        // all followers in cluster is removed. proposals on leader will write to it's local storage
        // but can never be committed
        List<byte[]> neverCommitDatas = TestUtil.newDataList(100, 100);
        TestUtil.randomPartitionList(neverCommitDatas).forEach(oldLeader::propose);

        // TODO how to know when logs have already been written to storage
        Thread.sleep(2000);
        cluster.shutdownPeer(oldLeader.getId());

        // add followers back. they will elect a new leader among themselves
        // proposals on the new leader will be committed
        followerStateMachines.forEach(s -> cluster.addTestingNode(s.getId(), peerIdSet).start());
        TestingRaftStateMachine newLeaderStateMachine = cluster.waitGetLeader();
        Raft newLeader = cluster.getNodeById(newLeaderStateMachine.getId());
        List<byte[]> dataList = TestUtil.newDataList(100, 100);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = newLeader.propose(batch);
            ProposalResponse p = resp.get(5000, TimeUnit.SECONDS);
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }

        for (TestingRaftStateMachine stateMachine : cluster.getAllStateMachines()) {
            checkAppliedLogs(stateMachine, stateMachine.getLastStatus().getTerm(), 101, dataList);
        }

        // add old leader back. logs on it's local storage will be overwritten by those logs synced from new leader
        cluster.addTestingNode(oldLeader.getId(), peerIdSet).start();
        TestingRaftStateMachine follower = cluster.getStateMachineById(oldLeader.getId());
        Future f = follower.becomeFollowerFuture();
        f.get();
        checkAppliedLogs(follower, follower.getLastStatus().getTerm(), 101, dataList);
    }
}
