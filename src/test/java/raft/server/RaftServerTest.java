package raft.server;

import org.junit.Test;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

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
        StateMachine leader = cluster.waitLeaderElected(2000);

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

    @Test
    public void testInitTwoNode() throws Exception {
        List<String> peers = new ArrayList<>();
        peers.add("two raft node 001");
        peers.add("two raft node 002");

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        StateMachine leader = cluster.waitLeaderElected();

        RaftStatus leaderStatus = leader.getStatus();
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(0, leaderStatus.getCommitIndex());
        assertEquals(0, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertNull(leaderStatus.getVotedFor());

        String followerId = leader.getId().equals(peers.get(0)) ? peers.get(1) : peers.get(0);
        StateMachine follower = cluster.getNodeById(followerId);
        ((TestingRaftCluster.TestingStateMachine)follower).waitBecomeFollower(2000);

        RaftStatus status = follower.getStatus();
        assertEquals(State.FOLLOWER, status.getState());
        assertEquals(0, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(leaderStatus.getTerm(), status.getTerm());
        assertEquals(leader.getId(), status.getLeaderId());

        cluster.shutdown();
    }

    @Test
    public void testInitTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("raft node 001");
        peerIdSet.add("raft node 002");
        peerIdSet.add("raft node 003");

        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
        StateMachine leader = cluster.waitLeaderElected(5000);

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
            TestingRaftCluster.TestingStateMachine follower = (TestingRaftCluster.TestingStateMachine)cluster.getNodeById(id);
            follower.waitBecomeFollower(2000);

            RaftStatus status = follower.getStatus();
            assertEquals(State.FOLLOWER, status.getState());
            assertEquals(0, status.getCommitIndex());
            assertEquals(0, status.getAppliedIndex());
            assertEquals(leaderStatus.getTerm(), status.getTerm());
            assertEquals(leaderId, status.getLeaderId());

            // if follower is convert from follower the votedFor is leaderId
            // if follower is convert from candidate by receiving ping from leader, the votedFort could be it self
            assertTrue((leaderId.equals(status.getVotedFor()))
                        || (status.getId().equals(status.getVotedFor())));
        }

        cluster.shutdown();
    }

    //TODO test several nodes become candidate simultaneously



    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "raft node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster cluster = new TestingRaftCluster(peers);
        StateMachine leader = cluster.waitLeaderElected(2000);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = RaftServerTest.newDataList(logCount);
        CompletableFuture<ProposeResponse> resp = leader.propose(dataList);
        ProposeResponse p = resp.get();
        assertEquals(selfId, p.getLeaderId());
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