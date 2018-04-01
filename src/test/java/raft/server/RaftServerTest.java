package raft.server;

import org.junit.Test;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
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

        // check raft status after propose logs
        RaftStatus status = leader.getStatus();
        assertEquals(selfId, status.getId());
        assertEquals(State.LEADER, status.getState());
        assertEquals(logCount, status.getCommitIndex());
        assertEquals(0, status.getAppliedIndex());
        assertEquals(1, status.getTerm());
        assertEquals(selfId, status.getLeaderId());
        assertNull(status.getVotedFor());

        // this is single node raft so proposed logs will be applied immediately so we can get applied logs from StateMachine
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