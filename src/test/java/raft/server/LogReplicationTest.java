package raft.server;

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
    @Before
    public void before() {
        TestingRaftCluster.cleanStorage();
    }

    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        TestingRaftCluster.init(peers);
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader();

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        CompletableFuture<ProposalResponse> resp = leader.propose(dataList);
        ProposalResponse p = resp.get();
        assertTrue(p.isSuccess());
        assertEquals(ErrorMsg.NONE, p.getError());

        // check raft status after logs proposed
        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertEquals(selfId, status.getVotedFor());

        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(selfId);
        List<LogEntry> applied  = stateMachine.drainAvailableApplied();
        for (LogEntry e : applied) {
            if (e != PersistentStorage.sentinel) {
                assertEquals(status.getTerm(), e.getTerm());
                assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
            }
        }

        TestingRaftCluster.shutdownCluster();
    }

    private static void checkAppliedLogs(RaftNode node, int logCount, List<byte[]> sourceDataList) {
        TestingRaftCluster.TestingRaftStateMachine stateMachine = TestingRaftCluster.getStateMachineById(node.getId());
        List<LogEntry> applied = stateMachine.waitApplied(logCount);

        // check node status after logs proposed
        RaftStatus status = node.getStatus();
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

        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
        RaftNode leader = TestingRaftCluster.waitGetLeader();

        String leaderId = leader.getId();
        HashSet<String> followerIds = new HashSet<>(peerIdSet);
        followerIds.remove(leaderId);

        // propose some logs
        int logCount = ThreadLocalRandom.current().nextInt(1, 10);
        List<byte[]> dataList = TestUtil.newDataList(logCount);
        assert logCount == dataList.size();
        CompletableFuture<ProposalResponse> resp = leader.propose(dataList);
        ProposalResponse p = resp.get();
        assertTrue(p.isSuccess());

        checkAppliedLogs(leader, logCount, dataList);
        for (String id : followerIds) {
            RaftNode node = TestingRaftCluster.getNodeById(id);
            checkAppliedLogs(node, logCount, dataList);
        }

        TestingRaftCluster.shutdownCluster();
    }

    // TODO test follower reject append entries
}
