package raft.server;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");
    private static final String persistentStateDir = "./target/deep/deep/deep/persistent";

    private Map<String, StateMachine> nodes = new HashMap<>();

    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            Config c = Config.newBuilder()
                    .withPeers(peers)
                    .withSelfID(peerId)
                    .withPersistentStateFileDirPath(persistentStateDir)
                    .build();

            TestingStateMachine stateMachine = new TestingStateMachine(c);
            nodes.put(peerId, stateMachine);
        }
    }

    void start() {
        nodes.values().forEach(StateMachine::start);
    }

    void clearPreviousPersistentState() throws Exception{
        TestUtil.cleanDirectory(Paths.get(persistentStateDir));
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
        private BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();

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
        public void onProposalCommited(List<LogEntry> msgs) {
            applied.addAll(msgs);
        }

        List<LogEntry> drainAvailableApplied() {
            List<LogEntry> ret = new ArrayList<>();
            this.applied.drainTo(ret);
            return Collections.unmodifiableList(ret);
        }

        List<LogEntry> waitApplied(long timeoutMs){
            return waitApplied(0, timeoutMs);
        }

        List<LogEntry> waitApplied(int atLeastExpect, long timeoutMs) {
            Preconditions.checkArgument(atLeastExpect >= 0);

            List<LogEntry> ret = new ArrayList<>(atLeastExpect);
            this.applied.drainTo(ret);

            long start = System.nanoTime();
            long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while ((atLeastExpect == 0 || ret.size() < atLeastExpect) && System.nanoTime() < deadline) {
                try {
                    LogEntry e;
                    if ((e = this.applied.poll(timeoutMs, TimeUnit.MILLISECONDS)) != null){
                        ret.add(e);
                        this.applied.drainTo(ret);
                    }
                } catch (InterruptedException ex) {
                    // ignore
                }
            }

            return Collections.unmodifiableList(ret);
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
