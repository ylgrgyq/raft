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
    @Test
    public void testInitSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster.init(peers);
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader();

        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(0, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertEquals(selfId, status.getVotedFor());

        TestingRaftCluster.shutdownCluster();
    }

    @Test
    public void testInitTwoNode() throws Exception {
        List<String> peers = new ArrayList<>();
        peers.add("double node 001");
        peers.add("double node 002");

        TestingRaftCluster.init(peers);
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader();

        RaftStatus leaderStatus = leader.getStatus();
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(0, leaderStatus.getCommitIndex());
        assertEquals(0, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(leader.getId(), leaderStatus.getVotedFor());

        String followerId = leader.getId().equals(peers.get(0)) ? peers.get(1) : peers.get(0);
        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(followerId);
        stateMachine.waitBecomeFollower().get();

        RaftNode follower = TestingRaftCluster.getNodeById(followerId);
        RaftStatus status = follower.getStatus();
        assertEquals(State.FOLLOWER, status.getState());
        assertEquals(0, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(leaderStatus.getTerm(), status.getTerm());
        assertEquals(leader.getId(), status.getLeaderId());

        TestingRaftCluster.shutdownCluster();
    }

    @Test
    public void testInitTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader();

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
        assertEquals(leaderId, leaderStatus.getVotedFor());

        for (String id : followerIds) {
            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(id);
            stateMachine.waitBecomeFollower().get();

            RaftNode follower = TestingRaftCluster.getNodeById(id);
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

        TestingRaftCluster.shutdownCluster();
    }

    @Test
    public void testLeaderLost() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader(5000);

        String leaderId = leader.getId();
        TestingRaftCluster.shutdownPeer(leaderId);
        peerIdSet.remove(leaderId);

        leader = TestingRaftCluster.waitGetLeader(5000);
        leaderId = leader.getId();
        RaftStatus leaderStatus = leader.getStatus();
        assertEquals(leaderId, leaderStatus.getId());
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(0, leaderStatus.getCommitIndex());
        assertEquals(0, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(leaderId, leaderStatus.getLeaderId());
        assertEquals(leaderId, leaderStatus.getVotedFor());

        peerIdSet.remove(leaderId);

        for (String id : peerIdSet) {
            TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(id);
            stateMachine.waitBecomeFollower().get();

            RaftNode follower = TestingRaftCluster.getNodeById(id);
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

        TestingRaftCluster.shutdownCluster();
    }

    // test leader lost then reelect a new leader then old leader comes back
}
