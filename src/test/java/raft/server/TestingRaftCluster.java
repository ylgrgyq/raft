package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");

    private Map<String, StateMachine> nodes = new HashMap<>();

    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            Config c = Config.newBuilder()
                    .withPeers(peers)
                    .withSelfID(peerId)
                    .build();

            TestingStateMachine stateMachine = new TestingStateMachine(c);
            stateMachine.start();
            nodes.put(peerId, stateMachine);
        }
    }

    StateMachine waitLeaderElected() throws TimeoutException, InterruptedException{
        return this.waitLeaderElected(0);
    }

    StateMachine waitLeaderElected(long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            for (StateMachine n : nodes.values()) {
                if (n.isLeader()) {
                    return n;
                }
            }
            if (timeoutMs != 0 && (System.currentTimeMillis() - start > timeoutMs)) {
                throw new TimeoutException();
            } else {
                Thread.sleep(200);
            }
        }
    }

    StateMachine getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    void shutdown(){
        for (StateMachine n : nodes.values()) {
            n.finish();
        }
    }

    class TestingStateMachine extends AbstractStateMachine{
        private List<LogEntry> applied = new ArrayList<>();

        TestingStateMachine(Config c){
            super(c);
        }

        @Override
        public void receiveCommand(RaftCommand cmd) {
            logger.debug("node {} receive command {}", this.getId(), cmd.toString());
            raftServer.queueReceivedCommand(cmd);
        }

        @Override
        public void onWriteCommand(RaftCommand cmd) {
            String to = cmd.getTo();
            logger.debug("node {} write command {}", this.getId(), cmd.toString());
            StateMachine toNode = nodes.get(to);
            toNode.receiveCommand(cmd);
        }

        @Override
        public void onProposalApplied(List<LogEntry> msgs) {
            applied.addAll(msgs);
        }

        List<LogEntry> getApplied(){
            return this.applied;
        }

        void waitBecomeFollower(long timeoutMs) throws TimeoutException, InterruptedException{
            long start = System.currentTimeMillis();
            while (true) {
                if (raftServer.getState() == State.FOLLOWER) {
                    return;
                }

                if (System.currentTimeMillis() - start > timeoutMs) {
                    throw new TimeoutException();
                } else {
                    Thread.sleep(200);
                }
            }
        }

    }
}
