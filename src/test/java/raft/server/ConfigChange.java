package raft.server;

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
public class ConfigChange {
//    @Test
//    public void testAddNodeToFollower() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        String newNode = "new node 004";
//        StateMachine follower = cluster.getNodeById(followerIds.iterator().next());
//        CompletableFuture<ProposeResponse> f = follower.addNode(newNode);
//        ProposeResponse resp = f.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());
//
//        peerIdSet.forEach(id -> {
//            StateMachine node = cluster.getNodeById(id);
//            RaftStatus status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testAddNode() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        String newNode = "new node 004";
//        CompletableFuture<ProposeResponse> f = leader.addNode(newNode);
//        ProposeResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//        assertNull(resp.getError());
//
//        peerIdSet.add(newNode);
//        peerIdSet.forEach(id -> {
//            TestingRaftCluster.TestingStateMachine node = cluster.getNodeById(id);
//            node.waitApplied(1, 5000);
//
//            RaftStatus status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testAddNodeSuccessively() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        String successNewNode = "success new node 004";
//        String failedNewNode = "failed new node 005";
//        CompletableFuture<ProposeResponse> f1 = leader.addNode(successNewNode);
//        CompletableFuture<ProposeResponse> f2 = leader.addNode(failedNewNode);
//        ProposeResponse resp = f1.get();
//        assertTrue(resp.isSuccess());
//        assertNull(resp.getError());
//        resp = f2.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION, resp.getError());
//
//        peerIdSet.add(successNewNode);
//        peerIdSet.forEach(id -> {
//            TestingRaftCluster.TestingStateMachine node = cluster.getNodeById(id);
//            node.waitApplied(1, 5000);
//
//            RaftStatus status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testRemoveNotExistsNode() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        String removePeerId = "not exists node";
//        CompletableFuture<ProposeResponse> f = leader.removeNode(removePeerId);
//        ProposeResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//        assertNull(resp.getError());
//
//        peerIdSet.forEach(id -> {
//            TestingRaftCluster.TestingStateMachine node = cluster.getNodeById(id);
//            node.waitApplied(1, 2000);
//
//            RaftStatus status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testRemoveFollower() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        String removePeerId = followerIds.iterator().next();
//        CompletableFuture<ProposeResponse> f = leader.removeNode(removePeerId);
//        ProposeResponse resp = f.get();
//        assertTrue(resp.isSuccess());
//        assertNull(resp.getError());
//
//        peerIdSet.remove(removePeerId);
//        peerIdSet.forEach(id -> {
//            TestingRaftCluster.TestingStateMachine node = cluster.getNodeById(id);
//            node.waitApplied(1, 5000);
//
//            RaftStatus status = node.getStatus();
//            List<String> peerIds = status.getPeerNodeIds();
//            assertEquals(peerIdSet.size(), peerIds.size());
//            assertTrue(peerIdSet.containsAll(peerIds));
//        });
//
//        assertNull(cluster.getNodeById(removePeerId));
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testRemoveLeader() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        CompletableFuture<ProposeResponse> f = leader.removeNode(leaderId);
//        ProposeResponse resp = f.get();
//        assertFalse(resp.isSuccess());
//        assertEquals(ErrorMsg.FORBID_REMOVE_LEADER, resp.getError());
//
//        cluster.shutdownCluster();
//    }

}
