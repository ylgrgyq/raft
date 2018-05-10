package raft.server;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

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

    private final Map<String, TestingNode> nodes = new ConcurrentHashMap<>();
    private final Config.ConfigBuilder configBuilder = Config.newBuilder()
                    .withPersistentStateFileDirPath(persistentStateDir);
    private final TestingBroker broker = new TestingBroker();


    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            nodes.put(peerId, new TestingNode(peers, peerId));
        }
    }

    void startCluster() {
        nodes.values().forEach(TestingNode::start);
    }

    TestingNode startPeer(String peerId) {
        TestingNode n = nodes.get(peerId);
        if (n != null) {
            n.start();
        } else {
            n = new TestingNode(nodes.keySet(), peerId);
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

    TestingNode waitLeaderElected() throws TimeoutException, InterruptedException{
        return this.waitLeaderElected(0);
    }

    TestingNode waitLeaderElected(long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            for (TestingNode n : nodes.values()) {
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

    TestingNode getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    void shutdownCluster(){
        for (TestingNode n : nodes.values()) {
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

    class TestingNode {
        private final RaftNode node;
        private final TestingRaftStateMachine stateMachine;

        TestingNode(Collection<String> peers, String selfId) {
            stateMachine = new TestingRaftStateMachine();

            Config c = configBuilder
                    .withPeers(peers)
                    .withSelfID(selfId)
                    .withRaftCommandBroker(broker)
                    .withStateMachine(stateMachine)
                    .build();
            node = new RaftNode(c);
        }

        void start() {
            node.start();
        }

        void receiveCommand(RaftCommand cmd) {
            node.receiveCommand(cmd);
        }

        void shutdown() {
            node.shutdown();
        }

        RaftStatus getStatus(){
            return node.getStatus();
        }

        boolean isLeader() {
            return node.isLeader();
        }

        String getId() {
            return node.getId();
        }

        void appliedTo(int appliedTo) {
            node.appliedTo(appliedTo);
        }

        CompletableFuture<ProposeResponse> propose(List<byte[]> data) {
            return node.propose(data);
        }

        List<LogEntry> drainAvailableApplied() {
            List<LogEntry> ret = new ArrayList<>();
            stateMachine.getApplied().drainTo(ret);
            return Collections.unmodifiableList(ret);
        }

        List<LogEntry> waitApplied(long timeoutMs){
            return waitApplied(0, timeoutMs);
        }

        List<LogEntry> waitApplied(int atLeastExpect, long timeoutMs) {
            Preconditions.checkArgument(atLeastExpect >= 0);

            List<LogEntry> ret = new ArrayList<>(atLeastExpect);
            stateMachine.getApplied().drainTo(ret);

            long start = System.nanoTime();
            long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while ((atLeastExpect == 0 || ret.size() < atLeastExpect) && System.nanoTime() < deadline) {
                try {
                    LogEntry e;
                    if ((e = stateMachine.getApplied().poll(timeoutMs, TimeUnit.MILLISECONDS)) != null){
                        ret.add(e);
                        stateMachine.getApplied().drainTo(ret);
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

    class TestingRaftStateMachine implements StateMachine{
        private final BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();

        TestingRaftStateMachine(){
        }

        @Override
        public void onNodeAdded(String peerId) {
            nodes.computeIfAbsent(peerId, k -> new TestingNode(nodes.keySet(), peerId));
            startPeer(peerId);
        }

        @Override
        public void onNodeRemoved(String peerId) {
            shutdownPeer(peerId);
        }

        @Override
        public void onProposalCommitted(List<LogEntry> msgs) {
            applied.addAll(msgs);
        }

        @Override
        public void onShutdown() {

        }

        BlockingQueue<LogEntry> getApplied() {
            return applied;
        }
    }

    class TestingBroker implements RaftCommandBroker {
        @Override
        public void onWriteCommand(RaftCommand cmd) {
            logger.debug("node {} write command {}", cmd.getFrom(), cmd.toString());
            String to = cmd.getTo();
            TestingNode toNode = nodes.get(to);
            if (toNode != null) {
                toNode.receiveCommand(cmd);
            }
        }
    }
}
