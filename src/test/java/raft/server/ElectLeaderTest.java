package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class ElectLeaderTest {
    private TestingRaftCluster cluster;

    @Before
    public void before() {
        cluster = new TestingRaftCluster(ElectLeaderTest.class.getSimpleName());
    }

    @After
    public void tearDown() {
        cluster.shutdownCluster();
        cluster.clearLogStorage();
        cluster.clearPersistentState();
    }

    @Test
    public void testInitSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        cluster.startCluster(peers);
        TestingRaftStateMachine leader = cluster.waitGetLeader();

        assertEquals(selfId, leader.getId());
        RaftStatusSnapshot status =  leader.getLastStatus();
        assertEquals(State.LEADER, status.getState());
        assertEquals(0, status.getCommitIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertEquals(selfId, status.getVotedFor());
    }

//    @Test
//    public void testInitTwoNode() throws Exception {
//        List<String> peers = new ArrayList<>();
//        peers.add("double node 001");
//        peers.add("double node 002");
//
//        TestingRaftCluster.init(peers);
//        TestingRaftCluster.clearPersistentState();
//        TestingRaftCluster.startCluster();
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        RaftStatusSnapshot leaderStatus = leader.getStatus();
//        assertEquals(State.LEADER, leaderStatus.getState());
//
//        // index of dummy log is 0, term is 0. but new leader can not commit logs from previous term by counting replicas
//        // so commit index remains -1
//        assertEquals(-1, leaderStatus.getCommitIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leader.getId(), leaderStatus.getVotedFor());
//
//        String followerId = leader.getId().equals(peers.get(0)) ? peers.get(1) : peers.get(0);
//        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(followerId);
//        stateMachine.waitBecomeFollower().get();
//
//        Raft follower = TestingRaftCluster.getNodeById(followerId);
//        RaftStatusSnapshot status = follower.getStatus();
//        assertEquals(State.FOLLOWER, status.getState());
//        assertEquals(-1, status.getCommitIndex());
//        assertEquals(-1, status.getAppliedIndex());
//        assertEquals(leaderStatus.getTerm(), status.getTerm());
//        assertEquals(leader.getId(), status.getLeaderId());
//
//        TestingRaftCluster.shutdownCluster();
//    }
//
//    @Test
//    public void testInitTripleNode() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
//        TestingRaftCluster.clearPersistentState();
//        TestingRaftCluster.startCluster();
//        Raft leader = TestingRaftCluster.waitGetLeader();
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        RaftStatusSnapshot leaderStatus = leader.getStatus();
//        assertEquals(leaderId, leaderStatus.getId());
//        assertEquals(State.LEADER, leaderStatus.getState());
//        // index of dummy log is 0, term is 0. but new leader can not commit logs from previous term by counting replicas
//        // so commit index remains -1
//        assertEquals(-1, leaderStatus.getCommitIndex());
//        assertEquals(-1, leaderStatus.getAppliedIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leaderId, leaderStatus.getLeaderId());
//        assertEquals(leaderId, leaderStatus.getVotedFor());
//
//        for (String id : followerIds) {
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(id);
//            stateMachine.waitBecomeFollower().get();
//
//            Raft follower = TestingRaftCluster.getNodeById(id);
//            RaftStatusSnapshot status = follower.getStatus();
//            assertEquals(State.FOLLOWER, status.getState());
//            assertEquals(-1, status.getCommitIndex());
//            assertEquals(-1, status.getAppliedIndex());
//            assertEquals(leaderStatus.getTerm(), status.getTerm());
//            assertEquals(leaderId, status.getLeaderId());
//
//            // if follower is convert from follower the votedFor is leaderId
//            // if follower is convert from candidate by receiving ping from leader, the votedFort could be it self
//            assertTrue((leaderId.equals(status.getVotedFor()))
//                    || (status.getId().equals(status.getVotedFor())));
//        }
//
//        TestingRaftCluster.shutdownCluster();
//    }
//
//    @Test
//    public void testLeaderLost() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("triple node 001");
//        peerIdSet.add("triple node 002");
//        peerIdSet.add("triple node 003");
//
//        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
//        TestingRaftCluster.clearPersistentState();
//        TestingRaftCluster.startCluster();
//        Raft leader = TestingRaftCluster.waitGetLeader(5000);
//
//        String leaderId = leader.getId();
//        TestingRaftCluster.shutdownPeer(leaderId);
//        peerIdSet.remove(leaderId);
//
//        leader = TestingRaftCluster.waitGetLeader(5000);
//        leaderId = leader.getId();
//        RaftStatusSnapshot leaderStatus = leader.getStatus();
//        assertEquals(leaderId, leaderStatus.getId());
//        assertEquals(State.LEADER, leaderStatus.getState());
//        assertEquals(-1, leaderStatus.getCommitIndex());
//        assertEquals(-1, leaderStatus.getAppliedIndex());
//        assertTrue(leaderStatus.getTerm() > 0);
//        assertEquals(leaderId, leaderStatus.getLeaderId());
//        assertEquals(leaderId, leaderStatus.getVotedFor());
//
//        peerIdSet.remove(leaderId);
//
//        for (String id : peerIdSet) {
//            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(id);
//            stateMachine.waitBecomeFollower().get();
//
//            Raft follower = TestingRaftCluster.getNodeById(id);
//            RaftStatusSnapshot status = follower.getStatus();
//            assertEquals(State.FOLLOWER, status.getState());
//            assertEquals(-1, status.getCommitIndex());
//            assertEquals(-1, status.getAppliedIndex());
//            assertEquals(leaderStatus.getTerm(), status.getTerm());
//            assertEquals(leaderId, status.getLeaderId());
//
//            // if follower is convert from follower the votedFor is leaderId
//            // if follower is convert from candidate by receiving ping from leader, the votedFort could be it self
//            assertTrue((leaderId.equals(status.getVotedFor()))
//                    || (status.getId().equals(status.getVotedFor())));
//        }
//
//        TestingRaftCluster.shutdownCluster();
//    }

    // test leader lost then reelect a new leader then old leader comes back
}
