package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class LogReplicationTest {
    private TestingRaftCluster cluster;

    @Before
    public void before() {
        cluster = new TestingRaftCluster(LogReplicationTest.class.getSimpleName());
        cluster.clearLogStorage();
        cluster.clearPersistentState();
    }

    @After
    public void tearDown() {
        cluster.shutdownCluster();
    }

    @Test
    public void testProposeOnSingleNode() throws Exception {
        String selfId = "single node 001";
        List<String> peers = new ArrayList<>();
        peers.add(selfId);

        cluster.startCluster(peers);
        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        List<byte[]> dataList = proposeSomeLogs(leader, 150);

        // check raft status after logs have been processed
        RaftStatusSnapshot status = leaderStateMachine.getLastStatus();
        assertEquals(State.LEADER, status.getState());
        assertEquals(dataList.size() + 1, status.getCommitIndex());
        assertEquals(1L, status.getTerm());
        assertEquals(selfId, status.getLeaderId());

        List<LogEntry> appliedEntries = leaderStateMachine.waitApplied(151);
        compareLogsWithSource(leaderStateMachine.getLastStatus().getTerm(), appliedEntries, dataList);
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

        List<byte[]> dataList = proposeSomeLogs(leader, 100);

        List<LogEntry> logsOnLeader = leaderStateMachine.waitApplied(101);
        compareLogsWithSource(leaderStateMachine.getLastStatus().getTerm(), logsOnLeader, dataList);
        compareLogsWithinCluster(logsOnLeader, cluster.getFollowers());
    }

    @Test
    public void testSyncLogOnRebootNode() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("reboot node 001");
        peerIdSet.add("reboot node 002");
        peerIdSet.add("reboot node 003");

        String rebootNodeId = "reboot node 003";
        cluster.addTestingNode("reboot node 001", peerIdSet).start();
        cluster.addTestingNode("reboot node 002", peerIdSet).start();
        assertEquals(2, cluster.getAllStateMachines().size());

        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        List<byte[]> dataList = proposeSomeLogs(leader, 100);

        long expectTerm = leaderStateMachine.getLastStatus().getTerm();
        List<LogEntry> logsOnLeader = leaderStateMachine.waitApplied(100);
        compareLogsWithSource(expectTerm, logsOnLeader, dataList);
        compareLogsWithinCluster(logsOnLeader, cluster.getFollowers());

        // reboot cluster including the reboot node
        cluster.shutdownCluster();
        cluster.startCluster(peerIdSet);
        leaderStateMachine = cluster.waitGetLeader();
        // reboot node do not have the latest logs so it can not be elected as leader
        assertNotEquals(rebootNodeId, leaderStateMachine.getId());

        // logs should sync to reboot node
        TestingRaftStateMachine follower = cluster.getStateMachineById(rebootNodeId);
        Future f = follower.becomeFollowerFuture();
        f.get();
        compareLogsWithinCluster(logsOnLeader,  Collections.singletonList(follower));
        checkCommitedIndex(leaderStateMachine);
    }

    @Test
    public void testOverwriteConflictLogs() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("overwrite conflict 001");
        peerIdSet.add("overwrite conflict 002");
        peerIdSet.add("overwrite conflict 003");

        cluster.startCluster(peerIdSet);
        TestingRaftStateMachine oldLeaderStateMachine = cluster.waitGetLeader();
        Raft oldLeader = cluster.getNodeById(oldLeaderStateMachine.getId());

        List<TestingRaftStateMachine> followerStateMachines = cluster.getFollowers();
        followerStateMachines.forEach(s -> cluster.shutdownPeer(s.getId()));

        // all followers in cluster is removed. proposals on leader will write to it's local storage
        // but will never be committed
        List<byte[]> neverCommitDatas = TestUtil.newDataList(100, 100);
        TestUtil.randomPartitionList(neverCommitDatas).forEach(oldLeader::propose);

        // TODO how to know when logs have already been written to storage
        Thread.sleep(2000);
        cluster.shutdownPeer(oldLeader.getId());

        // add followers back. they will elect a new leader among themselves
        // proposals on the new leader will be committed due to there are enough nodes online
        followerStateMachines.forEach(s -> cluster.addTestingNode(s.getId(), peerIdSet).start());
        TestingRaftStateMachine newLeaderStateMachine = cluster.waitGetLeader();
        Raft newLeader = cluster.getNodeById(newLeaderStateMachine.getId());

        List<byte[]> dataList = proposeSomeLogs(newLeader, 100);
        List<LogEntry> logsOnLeader = new ArrayList<>(newLeaderStateMachine.waitApplied(100));
        compareLogsWithSource(newLeaderStateMachine.getLastStatus().getTerm(), logsOnLeader, dataList);
        compareLogsWithinCluster(logsOnLeader, cluster.getFollowers());
        // restart cluster so new leader will initialize all follower's last index as 100
        cluster.shutdownCluster();
        cluster.startCluster(peerIdSet);

        newLeaderStateMachine = cluster.waitGetLeader();
        // logs on old leader's local storage will be overwritten by those logs synced from new leader
        TestingRaftStateMachine follower = cluster.getStateMachineById(oldLeader.getId());
        Future f = follower.becomeFollowerFuture();
        f.get();
        compareLogsWithinCluster(logsOnLeader, Collections.singletonList(follower));
        checkCommitedIndex(newLeaderStateMachine);
    }

    @Test
    public void testSnapshot() throws Exception {
        HashSet<String> peerIdSet = new HashSet<>();
        peerIdSet.add("snapshot 001");
        peerIdSet.add("snapshot 002");
        peerIdSet.add("snapshot 003");

        String fallBehindNodeId = "snapshot 003";
        cluster.addTestingNode("snapshot 001", peerIdSet).start();
        cluster.addTestingNode("snapshot 002", peerIdSet).start();
        assertEquals(2, cluster.getAllStateMachines().size());

        TestingRaftStateMachine leaderStateMachine = cluster.waitGetLeader();
        Raft leader = cluster.getNodeById(leaderStateMachine.getId());

        List<byte[]> dataList = new ArrayList<>(proposeSomeLogs(leader, 100));
        List<LogEntry> logsOnLeader = new ArrayList<>(leaderStateMachine.waitApplied(100));
        leaderStateMachine.flushMemtable();

        // propose some initial logs then schedule compaction job
        dataList.addAll(proposeSomeLogs(leader, 100));
        logsOnLeader.addAll(leaderStateMachine.waitApplied(100));
        long compactLogIndex = logsOnLeader.get(100).getIndex();
        leaderStateMachine.compact(compactLogIndex);
        // trigger compaction actual happen
        leaderStateMachine.flushMemtable();

        compareLogsWithSource(leaderStateMachine.getLastStatus().getTerm(), logsOnLeader, dataList);

        compareLogsWithinCluster(logsOnLeader, cluster.getFollowers());
        checkCommitedIndex(leaderStateMachine);

        CompletableFuture<LogSnapshot> expectSnapshot = leaderStateMachine.waitGetSnapshot();
        expectSnapshot.get();
        assertNotNull(expectSnapshot);

        // start fall behind node
        cluster.addTestingNode(fallBehindNodeId, peerIdSet).start();
        leaderStateMachine = cluster.waitGetLeader();
        assertNotEquals(fallBehindNodeId, leaderStateMachine.getId());

        // snapshot should sync to fall behind node
        TestingRaftStateMachine follower = cluster.getStateMachineById(fallBehindNodeId);
        CompletableFuture<Void> becomeFollower = follower.becomeFollowerFuture();
        CompletableFuture<LogSnapshot> waitSnapshot = follower.waitGetSnapshot();
        CompletableFuture.allOf(becomeFollower, waitSnapshot).get();
        assertEquals(expectSnapshot.get(), waitSnapshot.get());
        compareLogsWithinCluster(logsOnLeader.subList(101, logsOnLeader.size()),
                Collections.singletonList(follower));
    }

    private List<byte[]> proposeSomeLogs(Raft leader, int count) throws InterruptedException, ExecutionException {
        return proposeSomeLogs(leader, count, 100);
    }

    private List<byte[]> proposeSomeLogs(Raft leader, int count, int dataSize) throws InterruptedException, ExecutionException {
        List<byte[]> dataList = TestUtil.newDataList(count, dataSize);
        for (List<byte[]> batch : TestUtil.randomPartitionList(dataList)) {
            CompletableFuture<ProposalResponse> resp = leader.propose(batch);
            ProposalResponse p = resp.get();
            assertTrue(p.isSuccess());
            assertEquals(ErrorMsg.NONE, p.getError());
        }
        return dataList;
    }

    private void compareLogsWithSource(long expectTerm, List<LogEntry> logs, List<byte[]> sourceList) {
        logs = logs.stream().filter(e -> !e.getData().isEmpty()).collect(Collectors.toList());
        for (int i = 0; i < logs.size(); i++) {
            LogEntry e = logs.get(i);
            assertEquals(expectTerm, e.getTerm());
            assertArrayEquals(sourceList.get(i), e.getData().toByteArray());
        }
    }

    private void compareLogsWithinCluster(List<LogEntry> logsOnLeader, List<TestingRaftStateMachine> stateMachines) {
        for (TestingRaftStateMachine stateMachine : stateMachines) {
            List<LogEntry> applied = stateMachine.waitApplied(logsOnLeader.size());
            applied = applied.stream().filter(e -> !e.getData().isEmpty()).collect(Collectors.toList());
            // check node status after logs proposed
            assertEquals(logsOnLeader.size(), applied.size());

            for (int i = 0; i < logsOnLeader.size(); i++) {
                assertEquals(logsOnLeader.get(i), applied.get(i));
            }
        }
    }

    private void checkCommitedIndex(TestingRaftStateMachine leaderStateMachine){
        for (TestingRaftStateMachine sm : cluster.getAllStateMachines()){
            assertTrue(sm.getLastStatus().getCommitIndex() <= leaderStateMachine.getLastStatus().getCommitIndex());
        }
    }
}
