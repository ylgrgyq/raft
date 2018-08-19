package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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
    }

    @Test
    public void testInitTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leader = cluster.waitGetLeader();

        String leaderId = leader.getId();
        HashSet<String> expectFollowerIds = new HashSet<>(peerIdSet);
        expectFollowerIds.remove(leaderId);

        RaftStatusSnapshot leaderStatus = leader.getLastStatus();
        assertEquals(leaderId, leader.getId());
        assertEquals(State.LEADER, leaderStatus.getState());
        // index of dummy log is 0, term is 0. but new leader can not commit logs from previous term by counting replicas
        // so commit index remains -1
        assertEquals(-1, leaderStatus.getCommitIndex());
        assertEquals(-1, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(leaderId, leaderStatus.getLeaderId());

        List<TestingRaftStateMachine> actualFollowers = cluster.getFollowers();
        assertEquals(expectFollowerIds,
                actualFollowers.stream()
                        .map(TestingRaftStateMachine::getId)
                        .collect(Collectors.toSet()));

        for (TestingRaftStateMachine follower : actualFollowers) {
            Future f = follower.becomeFollowerFuture();
            f.get();
            RaftStatusSnapshot status = follower.getLastStatus();
            assertEquals(State.FOLLOWER, status.getState());
            assertEquals(-1, status.getCommitIndex());
            assertEquals(-1, status.getAppliedIndex());
            assertEquals(leaderStatus.getTerm(), status.getTerm());
            assertEquals(leaderId, status.getLeaderId());
        }
    }

    @Test
    public void testLeaderLost() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leader = cluster.waitGetLeader();

        String oldLeaderId = leader.getId();
        cluster.shutdownPeer(oldLeaderId);

        leader = cluster.waitGetLeader();
        String newLeaderId = leader.getId();
        RaftStatusSnapshot leaderStatus = leader.getLastStatus();
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(-1, leaderStatus.getCommitIndex());
        assertEquals(-1, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(newLeaderId, leaderStatus.getLeaderId());

        Set<String> expectFollowerIds = peerIdSet.stream()
                .filter(id -> !id.equals(oldLeaderId) && !id.equals(newLeaderId))
                .collect(Collectors.toSet());

        List<TestingRaftStateMachine> actualFollowers = cluster.getFollowers();
        assertEquals(expectFollowerIds,
                actualFollowers.stream()
                        .map(TestingRaftStateMachine::getId)
                        .collect(Collectors.toSet()));

        for (TestingRaftStateMachine follower : actualFollowers) {
            Future f = follower.becomeFollowerFuture();
            f.get();
            RaftStatusSnapshot status = follower.getLastStatus();
            assertEquals(State.FOLLOWER, status.getState());
            assertEquals(-1, status.getCommitIndex());
            assertEquals(-1, status.getAppliedIndex());
            assertEquals(leaderStatus.getTerm(), status.getTerm());
            assertEquals(newLeaderId, status.getLeaderId());
        }
    }

    @Test
    public void testLeaderReboot() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leader = cluster.waitGetLeader();

        String oldLeaderId = leader.getId();
        cluster.shutdownPeer(oldLeaderId);

        leader = cluster.waitGetLeader();
        String newLeaderId = leader.getId();
        RaftStatusSnapshot leaderStatus = leader.getLastStatus();
        assertEquals(State.LEADER, leaderStatus.getState());
        assertEquals(-1, leaderStatus.getCommitIndex());
        assertEquals(-1, leaderStatus.getAppliedIndex());
        assertTrue(leaderStatus.getTerm() > 0);
        assertEquals(newLeaderId, leaderStatus.getLeaderId());

        Raft oldLeader = cluster.addTestingNode(oldLeaderId, peerIdSet);
        oldLeader.start();


        Set<String> expectFollowerIds = peerIdSet.stream()
                .filter(id -> !id.equals(newLeaderId))
                .collect(Collectors.toSet());

        List<TestingRaftStateMachine> actualFollowers = cluster.getFollowers();
        assertEquals(expectFollowerIds,
                actualFollowers.stream()
                        .map(TestingRaftStateMachine::getId)
                        .collect(Collectors.toSet()));

        for (TestingRaftStateMachine follower : actualFollowers) {
            Future f = follower.becomeFollowerFuture();
            f.get();
            RaftStatusSnapshot status = follower.getLastStatus();
            assertEquals(State.FOLLOWER, status.getState());
            assertEquals(-1, status.getCommitIndex());
            assertEquals(-1, status.getAppliedIndex());
            assertEquals(leaderStatus.getTerm(), status.getTerm());
            assertEquals(newLeaderId, status.getLeaderId());
        }
    }
}
