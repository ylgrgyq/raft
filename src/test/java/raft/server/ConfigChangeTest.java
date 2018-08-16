package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/7
 */
public class ConfigChangeTest {
//    private HashSet<String> peerIdSet;
//    @Before
//    public void before() throws Exception {
//        peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster.cleanStorage();
//        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
//        TestingRaftCluster.clearPersistentState();
//        TestingRaftCluster.startCluster();
//    }
//
//    @After
//    public void after() throws Exception {
//        TestingRaftCluster.shutdownCluster();
//    }
//
//    @Test
//    public void testAddNodeToFollower() throws Exception {
//        String newNode = "new node 004";
//        Raft follower = TestingRaftCluster.getFollowers().get(0);
//        CompletableFuture<ProposalResponse> f = follower.addNode(newNode);
//        ProposalResponse resp = f.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());
//
//        peerIdSet.stream().map(TestingRaftCluster::getNodeById).forEach(node -> {
//            RaftStatusSnapshot status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//    }
//
//    @Test
//    public void testAddNode() throws Exception {
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String newNode = "new node 004";
//        CompletableFuture<ProposalResponse> f = leader.addNode(newNode);
//        ProposalResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//
//        HashSet<String> newPeerIds = new HashSet<>(peerIdSet);
//        newPeerIds.add(newNode);
//        peerIdSet.stream().map(TestingRaftCluster::getNodeById).forEach(node -> {
//            assertNotNull(node);
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(node.getId());
//
//            assertTrue(stateMachine.waitNodeAdded(newNode));
//
//            RaftStatusSnapshot status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(newPeerIds.size(), peerIds.size());
//            assertTrue(newPeerIds.containsAll(peerIds));
//        });
//    }
//
//    @Test
//    public void testAddNodeSuccessively() throws Exception {
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String successNewNode = "success new node 004";
//        String failedNewNode = "failed new node 005";
//        CompletableFuture<ProposalResponse> f1 = leader.addNode(successNewNode);
//        CompletableFuture<ProposalResponse> f2 = leader.addNode(failedNewNode);
//        ProposalResponse resp = f1.get();
//        assertTrue(resp.isSuccess());
//        resp = f2.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION, resp.getError());
//
//        peerIdSet.add(successNewNode);
//        peerIdSet.stream().map(TestingRaftCluster::getNodeById).forEach(node -> {
//            assertNotNull(node);
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(node.getId());
//            System.out.println(node.getId() + " start to wait node added");
//            stateMachine.waitNodeAdded(successNewNode);
//
//            RaftStatusSnapshot status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//    }
//
//    @Test
//    public void testRemoveNotExistsNode() throws Exception {
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String removePeerId = "not exists node";
//        CompletableFuture<ProposalResponse> f = leader.removeNode(removePeerId);
//        ProposalResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//
//        peerIdSet.stream().map(TestingRaftCluster::getNodeById).forEach(node -> {
//            assertNotNull(node);
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(node.getId());
//            stateMachine.waitNodeRemoved(removePeerId);
//
//            RaftStatusSnapshot status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//    }
//
//    @Test
//    public void testRemoveFollower() throws Exception {
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String removePeerId = TestingRaftCluster.getFollowers().get(0).getId();
//        CompletableFuture<ProposalResponse> f = leader.removeNode(removePeerId);
//        ProposalResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//
//        peerIdSet.remove(removePeerId);
//        peerIdSet.stream().map(TestingRaftCluster::getNodeById).forEach(node -> {
//            assertNotNull(node);
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(node.getId());
//            stateMachine.waitNodeRemoved(removePeerId);
//
//            RaftStatusSnapshot status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        assertNull(TestingRaftCluster.getNodeById(removePeerId));
//    }
//
//    @Test
//    public void testRemoveLeader() throws Exception {
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String leaderId = leader.getId();
//        CompletableFuture<ProposalResponse> f = leader.removeNode(leaderId);
//        ProposalResponse resp = f.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.FORBID_REMOVE_LEADER, resp.getError());
//    }

}
