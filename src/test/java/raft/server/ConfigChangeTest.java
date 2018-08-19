package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/7
 */
public class ConfigChangeTest {
    private HashSet<String> peerIdSet;
    private TestingRaftCluster cluster;

    @Before
    public void before() throws Exception {
        peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster = new TestingRaftCluster(ConfigChangeTest.class.getSimpleName());
        cluster.clearLogStorage();
        cluster.clearPersistentState();
        cluster.startCluster(peerIdSet);
    }

    @After
    public void after() throws Exception {
        cluster.shutdownCluster();
    }

    @Test
    public void testAddNodeToFollower() throws Exception {
        String newNode = "new node 004";
        TestingRaftStateMachine followerStateMachine = cluster.getFollowers().get(0);
        Raft follower = cluster.getNodeById(followerStateMachine.getId());

        CompletableFuture<ProposalResponse> f = follower.addNode(newNode);
        ProposalResponse resp = f.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());

        peerIdSet.stream().map(cluster::getStateMachineById).forEach(node -> {
            RaftStatusSnapshot status = node.getLastStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testAddNode() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String newNode = "new node 004";
        CompletableFuture<ProposalResponse> f = leader.addNode(newNode);
        ProposalResponse resp = f.get();
        assertTrue(resp.isSuccess());

        HashSet<String> newPeerIds = new HashSet<>(peerIdSet);
        newPeerIds.add(newNode);
        peerIdSet.stream().map(cluster::getStateMachineById).forEach(stateMachine -> {
            assertNotNull(stateMachine);

            assertTrue(stateMachine.waitNodeAdded(newNode));

            RaftStatusSnapshot status = stateMachine.getLastStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(newPeerIds.size(), peerIds.size());
            assertTrue(newPeerIds.containsAll(peerIds));
        });
    }

    @Test
    public void testAddNodeSuccessively() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String successNewNode = "success new node 004";
        String failedNewNode = "failed new node 005";

        // add a new node
        cluster.addTestingNode(successNewNode, cluster.getAllPeerIds()).start();
        cluster.addTestingNode(failedNewNode, cluster.getAllPeerIds()).start();

        CompletableFuture<ProposalResponse> f1 = leader.addNode(successNewNode);
        CompletableFuture<ProposalResponse> f2 = leader.addNode(failedNewNode);
        ProposalResponse resp = f1.get();
        assertTrue(resp.isSuccess());
        resp = f2.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION, resp.getError());

        peerIdSet.add(successNewNode);
        peerIdSet.stream().map(cluster::getStateMachineById).forEach(stateMachine -> {
            assertNotNull(stateMachine);

            System.out.println(stateMachine.getId() + " start to wait node added");
            stateMachine.waitNodeAdded(successNewNode);

            RaftStatusSnapshot status = stateMachine.getLastStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testRemoveNotExistsNode() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String removePeerId = "not exists node";
        CompletableFuture<ProposalResponse> f = leader.removeNode(removePeerId);
        ProposalResponse resp = f.get();
        assertTrue(resp.isSuccess());

        peerIdSet.stream().map(cluster::getStateMachineById).forEach(stateMachine -> {
            assertNotNull(stateMachine);

            stateMachine.waitNodeRemoved(removePeerId);

            RaftStatusSnapshot status = stateMachine.getLastStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testRemoveFollower() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String removePeerId = cluster.getFollowers().get(0).getId();
        CompletableFuture<ProposalResponse> f = leader.removeNode(removePeerId);
        ProposalResponse resp = f.get();
        assertTrue(resp.isSuccess());

        peerIdSet.remove(removePeerId);
        peerIdSet.stream().map(cluster::getStateMachineById).forEach(stateMachine -> {
            assertNotNull(stateMachine);
            stateMachine.waitNodeRemoved(removePeerId);

            RaftStatusSnapshot status = stateMachine.getLastStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });

        cluster.shutdownPeer(removePeerId);
        assertNull(cluster.getNodeById(removePeerId));
    }

    @Test
    public void testRemoveLeader() throws Exception {
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String leaderId = leader.getId();
        CompletableFuture<ProposalResponse> f = leader.removeNode(leaderId);
        ProposalResponse resp = f.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.FORBID_REMOVE_LEADER, resp.getError());
    }

}
