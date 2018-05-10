package raft.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.*;
import raft.server.proto.ConfigChange;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");
    private static final String persistentStateDir = "./target/deep/deep/deep/persistent";

    private final Map<String, TestingRaftNode> nodes = new ConcurrentHashMap<>();
    private final Config.ConfigBuilder configBuilder = Config.newBuilder()
                    .withPersistentStateFileDirPath(persistentStateDir);
    private final TestingBroker broker = new TestingBroker();

    class TestingBroker implements RaftCommandBroker {
        @Override
        public void onWriteCommand(RaftCommand cmd) {
            logger.debug("node {} write command {}", cmd.getFrom(), cmd.toString());
            String to = cmd.getTo();
            TestingRaftNode toNode = nodes.get(to);
            if (toNode != null) {
                toNode.receiveCommand(cmd);
            }
        }
    }

    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            Config.ConfigBuilder c = configBuilder
                    .withPeers(peers)
                    .withSelfID(peerId);

            TestingRaftNode node = new TestingRaftNode(c);
            nodes.put(peerId, node);
        }
    }

    void startCluster() {
        nodes.values().forEach(TestingRaftNode::start);
    }

    TestingRaftNode startPeer(String peerId) {
        TestingRaftNode n = nodes.get(peerId);
        if (n != null) {
            n.start();
        } else {
            Config.ConfigBuilder c = configBuilder
                    .withPeers(new ArrayList<>(nodes.keySet()))
                    .withSelfID(peerId);
            n = new TestingRaftNode(c);
            nodes.put(peerId, n);
            n.start();
        }

        return n;
    }

    void clearClusterPreviousPersistentState() {
        TestUtil.cleanDirectory(Paths.get(persistentStateDir));
    }

    void clearPreviousPersistentState(String peerId) {
        RaftPersistentState state = new RaftPersistentState(persistentStateDir, peerId);
        state.setTermAndVotedFor(0, null);
    }

    TestingRaftNode waitLeaderElected() throws TimeoutException, InterruptedException{
        return this.waitLeaderElected(0);
    }

    TestingRaftNode waitLeaderElected(long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            for (TestingRaftNode n : nodes.values()) {
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

    TestingRaftNode getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    void shutdownCluster(){
        for (TestingRaftNode n : nodes.values()) {
            n.shutdown();
        }
        nodes.clear();
    }

    void shutdownPeer(String peerId) {
        nodes.computeIfPresent(peerId, (k, n) -> {
            n.shutdown();
            return null;
        });
    }

    class TestingRaftNode implements StateMachine{
        private final BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();
        private RaftNode node;

        TestingRaftNode(Config.ConfigBuilder c){
            this.node = new RaftNode(c.withRaftCommandBroker(broker).withStateMachine(this).build());
        }

        void start () {
            node.start();
        }

        void shutdown() {
            node.shutdown();
        }

        boolean isLeader() {
            return node.isLeader();
        }

        void receiveCommand(RaftCommand cmd) {
            node.receiveCommand(cmd);
        }

        @Override
        public void onNodeAdded(String peerId) {
            Config.ConfigBuilder c = configBuilder
                    .withPeers(nodes.keySet())
                    .withSelfID(peerId);
            nodes.computeIfAbsent(peerId, k -> new TestingRaftNode(c));
            startPeer(peerId);
        }

        @Override
        public void onNodeRemoved(String peerId) {
            shutdownPeer(peerId);
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
                if (node.getState() == State.FOLLOWER) {
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
