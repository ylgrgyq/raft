package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

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

    @Test
    public void testImmediateTransfer() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft oldLeader = cluster.getNodeById(leaderStateMachine.getId());

        TestingRaftStateMachine newLeaderStateMachine = cluster.getFollowers().get(0);
        Raft newLeader = cluster.getNodeById(newLeaderStateMachine.getId());
        String newLeaderId = newLeader.getId();
        CompletableFuture<ProposalResponse> future = oldLeader.transferLeader(newLeaderId);
        ProposalResponse resp = future.get();
        assertTrue(resp.isSuccess());

        newLeaderStateMachine.becomeLeaderFuture().get();
        newLeaderStateMachine = cluster.waitGetLeader();
        assertEquals(newLeaderId, newLeaderStateMachine.getId());
        assertEquals(State.LEADER, newLeaderStateMachine.getLastStatus().getState());
    }

    @Test
    public void testTransferLeaderSuccessively() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft oldLeader = cluster.getNodeById(leaderStateMachine.getId());

        TestingRaftStateMachine newLeaderStateMachine = cluster.getFollowers().get(0);
        String newLeaderId = newLeaderStateMachine.getId();
        CompletableFuture<ProposalResponse> successFuture = oldLeader.transferLeader(newLeaderId);
        CompletableFuture<ProposalResponse> failedFuture = oldLeader.transferLeader(newLeaderId);
        assertTrue(successFuture.get().isSuccess());
        assertFalse(failedFuture.get().isSuccess());
        assertEquals(ErrorMsg.LEADER_TRANSFERRING, failedFuture.get().getError());

        TestingRaftStateMachine stateMachine = cluster.getStateMachineById(newLeaderId);
        stateMachine.becomeLeaderFuture().get();
        newLeaderStateMachine = cluster.waitGetLeader();
        assertEquals(newLeaderId, newLeaderStateMachine.getId());
        assertEquals(State.LEADER, newLeaderStateMachine.getLastStatus().getState());
    }

    @Test
    public void testTransferAfterCatchUp() throws Exception {
        TestingRaftStateMachine oldLeaderStateMachine = cluster.waitGetLeader();
        Raft oldLeader = cluster.getNodeById(oldLeaderStateMachine.getId());

        // propose some logs
        List<byte[]> dataList = TestUtil.newDataList(1, 100);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = oldLeader.propose(batch);
            ProposalResponse p = resp.get();
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }

        // add a new node
        String newLeaderId = "new node 004";
        Raft newNode = cluster.addTestingNode(newLeaderId, cluster.getAllPeerIds());
        newNode.start();

        CompletableFuture<ProposalResponse> f = oldLeader.addNode(newLeaderId);
        ProposalResponse resp = f.get();
        assertTrue(resp.isSuccess());
        oldLeaderStateMachine.waitNodeAdded(newLeaderId);

        // transfer leader to new node
        CompletableFuture<ProposalResponse> successFuture = oldLeader.transferLeader(newLeaderId);

        // propose will failed
        List<byte[]> failedData = TestUtil.newDataList(100);
        f = oldLeader.propose(failedData);
        ProposalResponse p = f.get(5000, TimeUnit.SECONDS);
        assertFalse(p.isSuccess());
        assertEquals(ErrorMsg.LEADER_TRANSFERRING, p.getError());

        // check leadership on new leader
        assertTrue(successFuture.get(5000, TimeUnit.SECONDS).isSuccess());
        TestingRaftStateMachine stateMachine = cluster.getStateMachineById(newLeaderId);
        stateMachine.becomeLeaderFuture().get(5000, TimeUnit.SECONDS);
        assertEquals(State.LEADER, stateMachine.getLastStatus().getState());
    }
}
