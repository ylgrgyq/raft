package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class LogReplicationTest {
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
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        cluster.startCluster(peers);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        CompletableFuture<ProposalResponse> resp = leader.propose(dataList);
        ProposalResponse p = resp.get();
        assertTrue(p.isSuccess());
        assertEquals(ErrorMsg.NONE, p.getError());

        // check raft status after logs proposed
        RaftStatusSnapshot status = leaderStateMachine.getLastStatus();
        assertEquals(State.LEADER, status.getState());
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());

        List<LogEntry> applied  = leaderStateMachine.drainAvailableApplied();
        for (LogEntry e : applied) {
            if (e != PersistentStorage.sentinel) {
                assertEquals(status.getTerm(), e.getTerm());
                assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
            }
        }
    }

    private static void checkAppliedLogs(TestingRaftStateMachine stateMachine, int logCount, List<byte[]> sourceDataList) {
        List<LogEntry> applied = stateMachine.waitApplied(logCount);

        // check node status after logs proposed
        RaftStatusSnapshot status = stateMachine.getLastStatus();
        assertEquals(logCount, status.getCommitIndex());

        for (LogEntry e : applied) {
            if (e != PersistentStorage.sentinel) {
                assertEquals(status.getTerm(), e.getTerm());
                assertArrayEquals(sourceDataList.get(e.getIndex() - 1), e.getData().toByteArray());
            }
        }
    }

    @Test
    public void testProposeOnTripleNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("triple node 001");
        peerIdSet.add("triple node 002");
        peerIdSet.add("triple node 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        String leaderId = leaderStateMachine.getId();
        HashSet<String> followerIds = new HashSet<>(peerIdSet);
        followerIds.remove(leaderId);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(1, 10);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        assert logCount == dataList.size();
        CompletableFuture<ProposalResponse> resp = leader.propose(dataList);
        ProposalResponse p = resp.get();
        assertTrue(p.isSuccess());

        checkAppliedLogs(leaderStateMachine, logCount, dataList);
        for (String id : followerIds) {
            TestingRaftStateMachine node = cluster.getStateMachineById(id);
            checkAppliedLogs(node, logCount, dataList);
        }
    }

    // TODO test follower reject append entries
}
