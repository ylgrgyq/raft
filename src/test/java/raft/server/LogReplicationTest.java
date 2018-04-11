package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class LogReplicationTest {
    //    @Test
//    public void testProposeOnTripleNode() throws Exception {
//        HashSet<String> peerIdSet = new HashSet<>();
//        peerIdSet.add("propose triple raft node 001");
//        peerIdSet.add("propose triple raft node 002");
//        peerIdSet.add("propose triple raft node 003");
//
//        TestingRaftCluster cluster = new TestingRaftCluster(new ArrayList<>(peerIdSet));
//        StateMachine leader = cluster.waitLeaderElected(5000);
//
//        String leaderId = leader.getId();
//        HashSet<String> followerIds = new HashSet<>(peerIdSet);
//        followerIds.remove(leaderId);
//
//        // propose some logs
//        int logCount = ThreadLocalRandom.current().nextInt(10, 100);
//        List<byte[]> dataList = RaftServerTest.newDataList(logCount);
//        CompletableFuture<ProposeResponse> resp = leader.propose(dataList);
//        ProposeResponse p = resp.get();
//        assertEquals(leaderId, p.getLeaderId());
//        assertTrue(p.isSuccess());
//        assertNull(p.getError());
//
//        // check raft status after logs proposed
//        RaftStatus status = leader.getStatus();
//        assertEquals(logCount, status.getCommitIndex());
//        assertEquals(0, status.getAppliedIndex());
//
//        // this is a single node raft so proposed logs will be applied immediately so we can get applied logs from StateMachine
//        List<LogEntry> applied = new ArrayList<>(((TestingRaftCluster.TestingStateMachine)leader).getApplied());
//        for (LogEntry e : applied) {
//            assertEquals(status.getTerm(), e.getTerm());
//            assertArrayEquals(dataList.get(e.getIndex() - 1), e.getData().toByteArray());
//        }
//
//        // check new raft status
//        leader.appliedTo(logCount);
//        RaftStatus newStatus = leader.getStatus();
//        assertEquals(logCount, newStatus.getAppliedIndex());
//
//        cluster.shutdown();
//    }
}
