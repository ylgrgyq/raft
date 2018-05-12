package raft.server;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/12
 */
public class LeaderTransferTest {
    private static final Logger logger = LoggerFactory.getLogger(LeaderTransferTest.class.getName());

    private HashSet<String> peerIdSet;
    @Before
    public void before() throws Exception {
        peerIdSet = new HashSet<>();
        peerIdSet.add("001");
        peerIdSet.add("002");
        peerIdSet.add("003");
        peerIdSet.add("004");
        peerIdSet.add("005");

        TestingRaftCluster.setStateMachineClass(TestingLeaderTransferStateMachine.class);
        TestingRaftCluster.init(new ArrayList<>(peerIdSet));
        TestingRaftCluster.clearClusterPreviousPersistentState();
        TestingRaftCluster.startCluster();
    }

    @After
    public void after() throws Exception {
        TestingRaftCluster.shutdownCluster();
    }

    static class TestingLeaderTransferStateMachine extends TestingRaftCluster.TestingRaftStateMachine {
        @Override
        public void onLeader() {
            logger.info("hahahaha");
        }
    }

    @Test
    public void testRequestTransferLeaderOnFollower() throws Exception {
        RaftNode follower = TestingRaftCluster.getFollowers().get(0);
        CompletableFuture<RaftResponse> future = follower.transferLeader("some node id");
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.NOT_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToLeader() throws Exception {
        RaftNode leader = TestingRaftCluster.waitGetLeader();
        CompletableFuture<RaftResponse> future = leader.transferLeader(leader.getId());
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.ALLREADY_LEADER, resp.getError());
    }

    @Test
    public void testTransferLeaderToUnknownPeerId() throws Exception {
        RaftNode leader = TestingRaftCluster.waitGetLeader();
        CompletableFuture<RaftResponse> future = leader.transferLeader("some node id");
        RaftResponse resp = future.get();
        assertFalse(resp.isSuccess());
        assertEquals(ErrorMsg.UNKNOWN_TRANSFEREEID, resp.getError());
    }

    @Test
    public void testImmediateTimeout() throws Exception {
        RaftNode leader = TestingRaftCluster.waitGetLeader();
        String newLeaderId = peerIdSet.iterator().next();
        CompletableFuture<RaftResponse> future = leader.transferLeader(newLeaderId);
        RaftResponse resp = future.get();
        assertTrue(resp.isSuccess());

        Thread.sleep(1000);
        RaftNode newLeader = TestingRaftCluster.waitGetLeader();
        assertEquals(newLeaderId, newLeader.getId());
    }

}
