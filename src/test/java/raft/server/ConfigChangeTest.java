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
    private TestingRaftCluster cluster;
    private HashSet<String> peerIdSet;
    @Before
    public void before() throws Exception {
        peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
        cluster.clearClusterPreviousPersistentState();
        cluster.startCluster();
    }

    @After
    public void after() throws Exception {
        cluster.shutdownCluster();
    }

    @Test
    public void testAddNodeToFollower() throws Exception {
        String newNode = "new node 004";
        RaftNode follower = cluster.getFollowers().get(0);
        CompletableFuture<RaftResponse> f = follower.addNode(newNode);
        RaftResponse resp = f.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());

        peerIdSet.stream().map(cluster::getNodeById).forEach(node -> {
            RaftStatus status = node.getStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testAddNode() throws Exception {
        RaftNode leader = cluster.waitLeaderElected();

        String newNode = "new node 004";
        CompletableFuture<RaftResponse> f = leader.addNode(newNode);
        RaftResponse resp = f.get();
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());

        peerIdSet.add(newNode);
        peerIdSet.stream().map(cluster::getNodeById).forEach(node -> {
            assertNotNull(node);
            cluster.waitApplied(node.getId(), 1);

            RaftStatus status = node.getStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testAddNodeSuccessively() throws Exception {
        RaftNode leader = cluster.waitLeaderElected();

        String successNewNode = "success new node 004";
        String failedNewNode = "failed new node 005";
        CompletableFuture<RaftResponse> f1 = leader.addNode(successNewNode);
        CompletableFuture<RaftResponse> f2 = leader.addNode(failedNewNode);
        RaftResponse resp = f1.get();
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());
        resp = f2.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION, resp.getError());

        peerIdSet.add(successNewNode);
        peerIdSet.stream().map(cluster::getNodeById).forEach(node -> {
            assertNotNull(node);
            cluster.waitApplied(node.getId(), 1);

            RaftStatus status = node.getStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testRemoveNotExistsNode() throws Exception {
        RaftNode leader = cluster.waitLeaderElected();

        String removePeerId = "not exists node";
        CompletableFuture<RaftResponse> f = leader.removeNode(removePeerId);
        RaftResponse resp = f.get();
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());

        peerIdSet.stream().map(cluster::getNodeById).forEach(node -> {
            assertNotNull(node);
            cluster.waitApplied(node.getId(), 1);

            RaftStatus status = node.getStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });
    }

    @Test
    public void testRemoveFollower() throws Exception {
        RaftNode leader = cluster.waitLeaderElected();

        String removePeerId = cluster.getFollowers().get(0).getId();
        CompletableFuture<RaftResponse> f = leader.removeNode(removePeerId);
        RaftResponse resp = f.get();
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());

        peerIdSet.remove(removePeerId);
        peerIdSet.stream().map(cluster::getNodeById).forEach(node -> {
            assertNotNull(node);
            cluster.waitApplied(node.getId(), 1);

            RaftStatus status = node.getStatus();
            List<String> peerIds = status.getPeerNodeIds();
            assertEquals(peerIdSet.size(), peerIds.size());
            assertTrue(peerIdSet.containsAll(peerIds));
        });

        assertNull(cluster.getNodeById(removePeerId));
    }

    @Test
    public void testRemoveLeader() throws Exception {
        RaftNode leader = cluster.waitLeaderElected();

        String leaderId = leader.getId();
        CompletableFuture<RaftResponse> f = leader.removeNode(leaderId);
        RaftResponse resp = f.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.FORBID_REMOVE_LEADER, resp.getError());
    }

}
