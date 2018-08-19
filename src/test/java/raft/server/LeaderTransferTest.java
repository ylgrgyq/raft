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
    private TestingRaftCluster cluster;

    @Before
    public void before() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("001");
        peerIdSet.add("002");
        peerIdSet.add("003");
        peerIdSet.add("004");
        peerIdSet.add("005");

        cluster = new TestingRaftCluster(LeaderTransferTest.class.getSimpleName());
        cluster.clearLogStorage();
        cluster.clearPersistentState();
        cluster.startCluster(peerIdSet);
    }

    @After
    public void after() throws Exception {
        cluster.shutdownCluster();
    }

    @Test
    public void testRequestTransferLeaderOnFollower() throws Exception {
        TestingRaftStateMachine followerStateMachine = cluster.getFollowers().get(0);
        Raft follower = cluster.getNodeById(followerStateMachine.getId());
        CompletableFuture<ProposalResponse> future = follower.transferLeader("some node id");
        ProposalResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToLeader() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());
        CompletableFuture<ProposalResponse> future = leader.transferLeader(leader.getId());
        ProposalResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.ALLREADY_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToUnknownPeerId() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());
        CompletableFuture<ProposalResponse> future = leader.transferLeader("some node id");
        ProposalResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.UNKNOWN_TRANSFEREEID, resp.getError());
    }

//    @Test
//    public void testImmediateTransfer() throws Exception {
//        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
//        Raft oldLeader = cluster.getNodeById(leaderStateMachine.getId());
//
//        TestingRaftStateMachine newLeaderStateMachine = cluster.getFollowers().get(0);
//        Raft newLeader = cluster.getNodeById(newLeaderStateMachine.getId());
//        String newLeaderId = newLeader.getId();
//        CompletableFuture<ProposalResponse> future = oldLeader.transferLeader(newLeaderId);
//        ProposalResponse resp = future.get();
//        assertTrue(resp.isSuccess());
//
//        newLeaderStateMachine.becomeFollowerFuture().get();
//        newLeaderStateMachine = cluster.waitGetLeader();
//        assertEquals(newLeaderId, newLeaderStateMachine.getId());
//        assertEquals(State.LEADER, newLeaderStateMachine.getLastStatus().getState());
//    }
//
//    @Test
//    public void testTransferLeaderSuccessively() throws Exception {
//        Raft oldLeader = TestingRaftCluster.waitGetLeader();
//        Raft newLeader = TestingRaftCluster.getFollowers().get(0);
//        String newLeaderId = newLeader.getId();
//        CompletableFuture<ProposalResponse> successFuture = oldLeader.transferLeader(newLeaderId);
//        CompletableFuture<ProposalResponse> failedFuture = oldLeader.transferLeader(newLeaderId);
//        assertTrue(successFuture.get().isSuccess());
//        assertFalse(failedFuture.get().isSuccess());
//        assertEquals(ErrorMsg.LEADER_TRANSFERRING, failedFuture.get().getError());
//
//        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(newLeaderId);
//        stateMachine.waitBecomeLeader().get();
//        newLeader = TestingRaftCluster.waitGetLeader();
//        assertEquals(newLeaderId, newLeader.getId());
//        assertEquals(State.LEADER, newLeader.getState());
//    }
//
//    @Test
//    public void testTransferAfterCatchUp() throws Exception {
//        Raft oldLeader = TestingRaftCluster.waitGetLeader();
//
//        // propose some logs
//        int proposeCount = 10  ;
//        for (int i = 0; i < proposeCount; i++) {
//            int logCount = ThreadLocalRandom.current().nextInt(10, 100);
//            List<byte[]> dataList = TestUtil.newDataList(logCount);
//            CompletableFuture<ProposalResponse> resp = oldLeader.propose(dataList);
//            ProposalResponse p = resp.get();
//            assertTrue(p.isSuccess());
//        }
//
//        // addFuture a new node
//        String newLeaderId = "new node 004";
//        CompletableFuture<ProposalResponse> f = oldLeader.addNode(newLeaderId);
//        ProposalResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//        TestingRaftCluster.TestingRaftStateMachine oldLeaderStateMachine =
//                TestingRaftCluster.getStateMachineById(oldLeader.getId());
//        oldLeaderStateMachine.waitNodeAdded(newLeaderId);
//
//        // transfer leader to new node
//        CompletableFuture<ProposalResponse> successFuture = oldLeader.transferLeader(newLeaderId);
//
//        // propose will failed
//        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
//        List<byte[]> dataList = TestUtil.newDataList(logCount);
//        f = oldLeader.propose(dataList);
//        ProposalResponse p = f.get();
//        assertFalse(p.isSuccess());
//        assertEquals(ErrorMsg.LEADER_TRANSFERRING, p.getError());
//
//        // check leadership on new leader
//        assertTrue(successFuture.get().isSuccess());
//        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(newLeaderId);
//        stateMachine.waitBecomeLeader().get();
//        Raft newLeader = TestingRaftCluster.waitGetLeader();
//        assertEquals(newLeaderId, newLeader.getId());
//        assertEquals(State.LEADER, newLeader.getState());
//    }
}
