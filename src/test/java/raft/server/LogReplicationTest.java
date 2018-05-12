package raft.server;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class LogReplicationTest {
    private static final Logger logger = LoggerFactory.getLogger(LogReplicationTest.class.getName());

    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        cluster.clearClusterPreviousPersistentState();
        cluster.startCluster();
        RaftNode leader = cluster.waitLeaderElected(2000);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        CompletableFuture<ProposeResponse> resp = leader.propose(dataList);
        ProposeResponse p = resp.get();
        assertEquals(selfId, p.getLeaderIdHint());
        assertTrue(p.isSuccess());
        assertNull(p.getError());

        // check raft status after logs proposed
        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertEquals(selfId, status.getVotedFor());

        // this is a single node raft so proposed logs will be applied immediately so we can get applied logs from StateMachine
        List<LogEntry> applied = cluster.drainAvailableApplied(selfId);
        for (LogEntry e : applied) {
            assertEquals(status.getTerm(), e.getTerm());
            assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
        }

        // check new raft status
        leader.appliedTo(logCount);
        RaftStatus newStatus = leader.getStatus();
        assertEquals(logCount, newStatus.getAppliedIndex());

        cluster.shutdownCluster();
    }

    private static void checkAppliedLogs(TestingRaftCluster cluster, RaftNode node, int logCount, List<byte[]> sourceDataList) {
        List<LogEntry> applied = cluster.waitApplied(node.getId(), logCount, 5000);

        // check node status after logs proposed
        RaftStatus status = node.getStatus();
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());

        for (LogEntry e : applied) {
            assertEquals(status.getTerm(), e.getTerm());
            assertArrayEquals(sourceDataList.get(e.getIndex() - 1), e.getData().toByteArray());
        }

        node.appliedTo(logCount);
        // check node status after applied
        RaftStatus newStatus = node.getStatus();
        assertEquals(logCount, newStatus.getAppliedIndex());
    }

    @Test
    public void testProposeOnTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
        cluster.clearClusterPreviousPersistentState();
        cluster.startCluster();
        RaftNode leader = cluster.waitLeaderElected(5000);

        String leaderId = leader.getId();
        HashSet<String> followerIds = new HashSet<>(peerIdSet);
        followerIds.remove(leaderId);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(1, 10);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        CompletableFuture<ProposeResponse> resp = leader.propose(dataList);
        ProposeResponse p = resp.get();
        assertEquals(leaderId, p.getLeaderIdHint());
        assertTrue(p.isSuccess());
        assertNull(p.getError());

        checkAppliedLogs(cluster, leader, logCount, dataList);
        for (String id : followerIds) {
            RaftNode node = cluster.getNodeById(id);
            checkAppliedLogs(cluster, node, logCount, dataList);
        }

        cluster.shutdownCluster();
    }

    // TODO test follower reject append entries
}
