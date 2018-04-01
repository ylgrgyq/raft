package raft.server;

import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
public class TestingRaftCluster {
    private List<StateMachine> nodes = new ArrayList<>();

    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            Config c = Config.newBuilder()
                    .withPeers(peers)
                    .withSelfID(peerId)
                    .build();

            TestingStateMachine stateMachine = new TestingStateMachine(c);
            stateMachine.start();
            nodes.add(stateMachine);
        }
    }

    public StateMachine waitLeaderElected(long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            for (StateMachine n : nodes) {
                if (n.isLeader()) {
                    return n;
                }
            }
            if (System.currentTimeMillis() - start > timeoutMs) {
                throw new TimeoutException();
            } else {
                Thread.sleep(200);
            }
        }
    }

    public void shutdown(){
        for (StateMachine n : nodes) {
            n.finish();
        }
    }

    public static class TestingStateMachine extends AbstractStateMachine{
        private List<LogEntry> applied = new ArrayList<>();

        TestingStateMachine(Config c){
            super(c);
        }

        @Override
        public void onWriteCommand(RaftCommand cmd) {

        }

        @Override
        public void onProposalApplied(List<LogEntry> msgs) {
            applied.addAll(msgs);
        }

        List<LogEntry> getApplied(){
            return this.applied;
        }


    }
}
