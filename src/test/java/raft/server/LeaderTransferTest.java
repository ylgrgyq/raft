package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/12
 */
public class LeaderTransferTest {
    @Before
    public void before() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("001");
        peerIdSet.add("002");
        peerIdSet.add("003");
        peerIdSet.add("004");
        peerIdSet.add("005");

        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
    }

    @After
    public void after() throws Exception {
        TestingRaftCluster.shutdownCluster();
    }

    @Test
    public void testRequestTransferLeaderOnFollower() throws Exception {
        RaftNode follower = TestingRaftCluster.getFollowers().get(0);
        CompletableFuture<RaftResponse> future = follower.transferLeader("some node id");
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToLeader() throws Exception {
        RaftNode leader = TestingRaftCluster.waitGetLeader();
        CompletableFuture<RaftResponse> future = leader.transferLeader(leader.getId());
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.ALLREADY_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToUnknownPeerId() throws Exception {
        RaftNode leader = TestingRaftCluster.waitGetLeader();
        CompletableFuture<RaftResponse> future = leader.transferLeader("some node id");
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.UNKNOWN_TRANSFEREEID, resp.getError());
    }

    @Test
    public void testImmediateTransfer() throws Exception {
        RaftNode oldLeader = TestingRaftCluster.waitGetLeader();
        RaftNode newLeader = TestingRaftCluster.getFollowers().get(0);
        String newLeaderId = newLeader.getId();
        CompletableFuture<RaftResponse> future = oldLeader.transferLeader(newLeaderId);
        RaftResponse resp = future.get();
        assertNull(resp.getError());
        assertTrue(resp.isSuccess());

        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(newLeaderId);
        stateMachine.waitBecomeLeader().get();
        newLeader = TestingRaftCluster.waitGetLeader();
        assertEquals(newLeaderId, newLeader.getId());
        assertEquals(State.LEADER, newLeader.getState());
    }

    @Test
    public void testTransferLeaderSuccessively() throws Exception {
        RaftNode oldLeader = TestingRaftCluster.waitGetLeader();
        RaftNode newLeader = TestingRaftCluster.getFollowers().get(0);
        String newLeaderId = newLeader.getId();
        CompletableFuture<RaftResponse> successFuture = oldLeader.transferLeader(newLeaderId);
        CompletableFuture<RaftResponse> failedFuture = oldLeader.transferLeader(newLeaderId);
        assertNull(successFuture.get().getError());
        assertTrue(successFuture.get().isSuccess());
        assertFalse(failedFuture.get().isSuccess());
        assertEquals(ErrorMsg.LEADER_TRANSFERRING, failedFuture.get().getError());

        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(newLeaderId);
        stateMachine.waitBecomeLeader().get();
        newLeader = TestingRaftCluster.waitGetLeader();
        assertEquals(newLeaderId, newLeader.getId());
        assertEquals(State.LEADER, newLeader.getState());
    }

    @Test
    public void testTransferAfterCatchUp() throws Exception {
        RaftNode oldLeader = TestingRaftCluster.waitGetLeader();

        // propose some logs
        int proposeCount = 10  ;
        for (int i = 0; i < proposeCount; i++) {
            int logCount = ThreadLocalRandom.current().nextInt(10, 100);
            List<byte[]> dataList = TestUtil.newDataList(logCount);
            CompletableFuture<RaftResponse> resp = oldLeader.propose(dataList);
            RaftResponse p = resp.get();
            assertTrue(p.isSuccess());
            assertNull(p.getError());
        }

        // add a new node
        String newLeaderId = "new node 004";
        CompletableFuture<RaftResponse> f = oldLeader.addNode(newLeaderId);
        RaftResponse resp = f.get();
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());
        TestingRaftCluster.TestingRaftStateMachine oldLeaderStateMachine =
                TestingRaftCluster.getStateMachineById(oldLeader.getId());
        oldLeaderStateMachine.waitNodeAdded(newLeaderId);

        // transfer leader to new node
        CompletableFuture<RaftResponse> successFuture = oldLeader.transferLeader(newLeaderId);
        assertNull(successFuture.get().getError());
        assertTrue(successFuture.get().isSuccess());

        // propose will failed
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        f = oldLeader.propose(dataList);
        RaftResponse p = f.get();
        assertFalse(p.isSuccess());
        assertEquals(ErrorMsg.LEADER_TRANSFERRING, p.getError());

        // check leadership on new leader
        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(newLeaderId);
        stateMachine.waitBecomeLeader().get();
        RaftNode newLeader = TestingRaftCluster.waitGetLeader();
        assertEquals(newLeaderId, newLeader.getId());
        assertEquals(State.LEADER, newLeader.getState());
    }
}
