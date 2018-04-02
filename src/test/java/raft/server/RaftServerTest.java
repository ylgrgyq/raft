package raft.server;

import org.junit.Test;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/1/24
 */
public class RaftServerTest {
    private static List<byte[]> newDataList(int count) {
        List<byte[]> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] data = new byte[5];
            ThreadLocalRandom.current().nextBytes(data);
            dataList.add(data);
        }
        return dataList;
    }

    @Test
    public void testInitSingleNode() throws Exception {
        String selfId = "raft node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        StateMachine leader = cluster.waitLeaderElected(1000);

        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(0, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertNull(status.getVotedFor());

        cluster.shutdown();
    }

    @Test(expected = TimeoutException.class)
    public void testInitTwoNode() throws Exception {
        List<String> peers = new ArrayList<>();
        peers.add("raft node 001");
        peers.add("raft node 002");

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        cluster.waitLeaderElected(2000);
        throw new RuntimeException("Can't elect a leader with two nodes");
    }

    @Test
    public void testInitTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("raft node 001");
        peerIdSet.add("raft node 002");
        peerIdSet.add("raft node 003");

        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
        StateMachine leader = cluster.waitLeaderElected(2000);

        String leaderId = leader.getId();
        HashSet<String> followerIds = new HashSet<>(peerIdSet);
        followerIds.remove(leaderId);

        RaftStatus leaderStatus = leader.getStatus();
        assertEquals(leaderId, leaderStatus.getId());
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(0, leaderStatus.getCommitIndex());
        assertEquals(0, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(leaderId, leaderStatus.getLeaderId());
        assertNull(leaderStatus.getVotedFor());

        for (String id : followerIds) {
            StateMachine follower = cluster.getNodeById(id);
            RaftStatus status = follower.getStatus();
            assertEquals(leaderId, status.getId());
            assertEquals(State.FOLLOWER, status.getState());
            assertEquals(0, status.getCommitIndex());
            assertEquals(0, status.getAppliedIndex());
            assertEquals(leaderStatus.getTerm(), status.getTerm());
            assertEquals(leaderId, status.getLeaderId());
            assertNull(status.getVotedFor());
        }

        cluster.shutdown();
    }

    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "raft node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        StateMachine leader = cluster.waitLeaderElected(1000);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = RaftServerTest.newDataList(logCount);
        ProposeResponse resp = leader.propose(dataList);
        assertEquals(selfId, resp.getLeaderId());
        assertTrue(resp.isSuccess());
        assertNull(resp.getError());

        // check raft status after logs proposed
        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertNull(status.getVotedFor());

        // this is a single node raft so proposed logs will be applied immediately so we can get applied logs from StateMachine
        List<LogEntry> applied = new ArrayList<>(((TestingRaftCluster.TestingStateMachine)leader).getApplied());
        for (LogEntry e : applied) {
            assertEquals(status.getTerm(), e.getTerm());
            assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
        }

        // check new raft status
        leader.appliedTo(logCount);
        RaftStatus newStatus = leader.getStatus();
        assertEquals(logCount, newStatus.getAppliedIndex());

        cluster.shutdown();
    }

}