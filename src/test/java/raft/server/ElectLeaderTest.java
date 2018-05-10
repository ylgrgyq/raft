package raft.server;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class ElectLeaderTest {
//    @Test
//    public void testInitSingleNode() throws Exception {
//        String selfId = "single node 001";
//        List<String> peers = new ArrayList<>();
//        peers.add(selfId);
//
//        TestingRaftCluster cluster = new TestingRaftCluster(peers);
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected(2000);
//
//        RaftStatus status = leader.getStatus();
//        assertEquals(selfId, status.getId());
//        assertEquals(State.LEADER, status.getState());
//        assertEquals(0, status.getCommitIndex());
//        assertEquals(0, status.getAppliedIndex());
//        assertEquals(1, status.getTerm());
//        assertEquals(selfId, status.getLeaderId());
//        assertEquals(selfId, status.getVotedFor());
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testInitTwoNode() throws Exception {
//        List<String> peers = new ArrayList<>();
//        peers.add("double node 001");
//        peers.add("double node 002");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(peers);
//        cluster.clearClusterPreviousPersistentState();
//        cluster.startCluster();
//        StateMachine leader = cluster.waitLeaderElected();
//
//        RaftStatus leaderStatus = leader.getStatus();
//        assertEquals(State.LEADER, leaderStatus.getState());
//        assertEquals(0, leaderStatus.getCommitIndex());
//        assertEquals(0, leaderStatus.getAppliedIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leader.getId(), leaderStatus.getVotedFor());
//
//        String followerId = leader.getId().equals(peers.get(0)) ? peers.get(1) : peers.get(0);
//        StateMachine follower = cluster.getNodeById(followerId);
//        ((TestingRaftCluster.TestingStateMachine)follower).waitBecomeFollower(2000);
//
//        RaftStatus status = follower.getStatus();
//        assertEquals(State.FOLLOWER, status.getState());
//        assertEquals(0, status.getCommitIndex());
//        assertEquals(0, status.getAppliedIndex());
//        assertEquals(leaderStatus.getTerm(), status.getTerm());
//        assertEquals(leader.getId(), status.getLeaderId());
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testInitTripleNode() throws Exception {
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
//        RaftStatus leaderStatus = leader.getStatus();
//        assertEquals(leaderId, leaderStatus.getId());
//        assertEquals(State.LEADER, leaderStatus.getState());
//        assertEquals(0, leaderStatus.getCommitIndex());
//        assertEquals(0, leaderStatus.getAppliedIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leaderId, leaderStatus.getLeaderId());
//        assertEquals(leaderId, leaderStatus.getVotedFor());
//
//        for (String id : followerIds) {
//            TestingRaftCluster.TestingStateMachine follower = (TestingRaftCluster.TestingStateMachine)cluster.getNodeById(id);
//            follower.waitBecomeFollower(2000);
//
//            RaftStatus status = follower.getStatus();
//            assertEquals(State.FOLLOWER, status.getState());
//            assertEquals(0, status.getCommitIndex());
//            assertEquals(0, status.getAppliedIndex());
//            assertEquals(leaderStatus.getTerm(), status.getTerm());
//            assertEquals(leaderId, status.getLeaderId());
//
//            // if follower is convert from follower the votedFor is leaderId
//            // if follower is convert from candidate by receiving ping from leader, the votedFort could be it self
//            assertTrue((leaderId.equals(status.getVotedFor()))
//                    || (status.getId().equals(status.getVotedFor())));
//        }
//
//        cluster.shutdownCluster();
//    }
//
//    @Test
//    public void testLeaderLost() throws Exception {
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
//        cluster.shutdownPeer(leaderId);
//        peerIdSet.remove(leaderId);
//
//        leader = cluster.waitLeaderElected(5000);
//        leaderId = leader.getId();
//        RaftStatus leaderStatus = leader.getStatus();
//        assertEquals(leaderId, leaderStatus.getId());
//        assertEquals(State.LEADER, leaderStatus.getState());
//        assertEquals(0, leaderStatus.getCommitIndex());
//        assertEquals(0, leaderStatus.getAppliedIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leaderId, leaderStatus.getLeaderId());
//        assertEquals(leaderId, leaderStatus.getVotedFor());
//
//        peerIdSet.remove(leaderId);
//
//        for (String id : peerIdSet) {
//            TestingRaftCluster.TestingStateMachine follower = (TestingRaftCluster.TestingStateMachine)cluster.getNodeById(id);
//            follower.waitBecomeFollower(2000);
//
//            RaftStatus status = follower.getStatus();
//            assertEquals(State.FOLLOWER, status.getState());
//            assertEquals(0, status.getCommitIndex());
//            assertEquals(0, status.getAppliedIndex());
//            assertEquals(leaderStatus.getTerm(), status.getTerm());
//            assertEquals(leaderId, status.getLeaderId());
//
//            // if follower is convert from follower the votedFor is leaderId
//            // if follower is convert from candidate by receiving ping from leader, the votedFort could be it self
//            assertTrue((leaderId.equals(status.getVotedFor()))
//                    || (status.getId().equals(status.getVotedFor())));
//        }
//
//        cluster.shutdownCluster();
//    }

    // test leader lost then reelect a new leader then old leader comes back
}
