package raft.server;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;
import raft.server.proto.Snapshot;

import java.lang.reflect.Array;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");
    private static final long defaultTimeoutMs = 5000;
    private static final String persistentStateDir = "./target/deep/deep/deep/persistent";
    private static final Config.ConfigBuilder configBuilder = Config.newBuilder()
            .withPersistentStateFileDirPath(persistentStateDir);
    private static final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private static final Map<String, TestingRaftStateMachine> stateMachines = new ConcurrentHashMap<>();
    private static final TestingBroker broker = new TestingBroker();

    static void init(List<String> peers) {
        for (String peerId : peers) {
            addTestingNode(peerId, peers);
        }
    }

    static RaftNode addTestingNode(String peerId, Collection<String> peers) {
        return nodes.computeIfAbsent(peerId, k -> createTestingNode(peerId, peers));
    }

    private static RaftNode createTestingNode(String selfId, Collection<String> peers) {
        TestingRaftStateMachine stateMachine = new TestingRaftStateMachine();
        stateMachines.put(selfId, stateMachine);

        Config c = configBuilder
                .withPeers(peers)
                .withSelfID(selfId)
                .withRaftCommandBroker(broker)
                .withStateMachine(stateMachine)
                .withPersistentStorage(new MemoryFakePersistentStorage())
                .build();
        return new RaftNode(c);
    }

    static void startCluster() {
        nodes.values().forEach(RaftNode::start);
    }

    static void clearClusterPreviousPersistentState() {
        TestUtil.cleanDirectory(Paths.get(persistentStateDir));
    }

    static void clearPreviousPersistentStateFor(String peerId) {
        RaftPersistentState state = new RaftPersistentState(persistentStateDir, peerId);
        state.setTermAndVotedFor(0, null);
    }

    static RaftNode waitGetLeader() throws TimeoutException, InterruptedException {
        return waitGetLeader(defaultTimeoutMs);
    }

    static RaftNode waitGetLeader(long timeoutMs) throws TimeoutException, InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            ArrayList<RaftNode> possibleLeaderNodes = new ArrayList<>(nodes.size());
            for (RaftNode n : nodes.values()) {
                if (n.isLeader()) {
                    possibleLeaderNodes.add(n);
                }
            }

            if (possibleLeaderNodes.size() != 0) {
                if (possibleLeaderNodes.size() == 1) {
                    return possibleLeaderNodes.get(0);
                }

                // we could have more than one leader but they must have different term.
                // the scenario is we have A B C three nodes, A was the old leader and now B C elect B as new leader.
                // Before B had a chance to notify A it is the new leader, A continue to consider itself as leader. At
                // this moment we have two leader A and B but they don't share the same term. B's term must
                // be greater than A's.
                logger.warn("we have more than one leader: {}", possibleLeaderNodes);
            }

            if (timeoutMs != 0 && (System.currentTimeMillis() - start > timeoutMs)) {
                throw new TimeoutException();
            } else {
                Thread.sleep(200);
            }
        }
    }

    static List<RaftNode> getFollowers() throws TimeoutException, InterruptedException {
        waitGetLeader();
        ArrayList<RaftNode> followers = new ArrayList<>();
        for (Map.Entry<String, RaftNode> e : nodes.entrySet()) {
            RaftNode n = e.getValue();
            if (!n.isLeader()) {
                followers.add(e.getValue());
            }
        }
        return followers;
    }

    static RaftNode getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    static TestingRaftStateMachine getStateMachineById(String peerId) {
        TestingRaftStateMachine stateMachine = stateMachines.get(peerId);
        if (stateMachine != null) {
            return stateMachine;
        }
        throw new RuntimeException("no state machine for " + peerId);
    }

    static void shutdownCluster() {
        for (RaftNode n : nodes.values()) {
            n.shutdown();
        }
        nodes.clear();
    }

    static void shutdownPeer(String peerId) {
        nodes.computeIfPresent(peerId, (k, n) -> {
            n.shutdown();
            return null;
        });
    }

    static class TestingRaftStateMachine implements StateMachine {
        private final BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();
        private AtomicBoolean isLeader = new AtomicBoolean(false);
        private AtomicBoolean isFollower = new AtomicBoolean(false);
        private CompletableFuture<Void> waitLeaderFuture;
        private CompletableFuture<Void> waitFollowerFuture;
        private BlockingQueue<String> nodeAdded = new LinkedBlockingQueue<>();
        private BlockingQueue<String> nodeRemoved = new LinkedBlockingQueue<>();

        @Override
        public void onNodeAdded(String peerId) {
            logger.info("on node added called " + peerId);
            nodeAdded.add(peerId);
            addTestingNode(peerId, nodes.keySet()).start();
        }

        @Override
        public void onNodeRemoved(String peerId) {
            nodeRemoved.add(peerId);
            shutdownPeer(peerId);
        }

        @Override
        public void onProposalCommitted(List<LogEntry> msgs) {
            assert msgs != null && !msgs.isEmpty() : "msgs is null:" + (msgs == null);
            applied.addAll(msgs);
        }

        @Override
        public void installSnapshot(Snapshot snap) {

        }

        @Override
        public Optional<Snapshot> getRecentSnapshot(int expectIndex) {
            return null;
        }

        @Override
        public void onLeaderStart(int term) {
            isLeader.set(true);
            if (waitLeaderFuture != null) {
                waitLeaderFuture.complete(null);
            }
        }

        @Override
        public void onLeaderFinish() {
            isLeader.set(false);
        }

        CompletableFuture<Void> waitBecomeLeader() {
            if (isLeader.get()) {
                return CompletableFuture.completedFuture(null);
            } else {
                waitLeaderFuture = new CompletableFuture<>();
                return waitLeaderFuture;
            }
        }

        @Override
        public void onFollowerStart(int term, String leaderId) {
            isFollower.set(true);
            if (waitFollowerFuture != null) {
                waitFollowerFuture.complete(null);
            }
        }

        @Override
        public void onFollowerFinish() {
            isFollower.set(false);
        }

        CompletableFuture<Void> waitBecomeFollower() {
            if (isFollower.get()) {
                return CompletableFuture.completedFuture(null);
            } else {
                waitFollowerFuture = new CompletableFuture<>();
                return waitFollowerFuture;
            }
        }

        @Override
        public void onShutdown() {
            logger.info("state machine shutdown");
        }

        BlockingQueue<LogEntry> getApplied() {
            return applied;
        }

        private boolean doWaitNodeChanged(String expectPeerId, BlockingQueue<String> queue) {
            assert expectPeerId != null && !expectPeerId.isEmpty();

            try {
                String id = queue.poll(defaultTimeoutMs, TimeUnit.MILLISECONDS);
                return expectPeerId.equals(id);
            } catch (InterruptedException ex) {
                // ignore
            }

            return false;
        }

        boolean waitNodeAdded(String expectPeerId) {
            return doWaitNodeChanged(expectPeerId, nodeAdded);
        }

        boolean waitNodeRemoved(String expectPeerId) {
            return doWaitNodeChanged(expectPeerId, nodeRemoved);
        }

        List<LogEntry> waitApplied(int atLeastExpect) {
            return waitApplied(atLeastExpect, defaultTimeoutMs);
        }

        List<LogEntry> waitApplied(int atLeastExpect, long timeoutMs) {
            assert atLeastExpect >= 0 : "actual " + atLeastExpect;

            List<LogEntry> ret = new ArrayList<>(atLeastExpect);
            getApplied().drainTo(ret);

            long start = System.nanoTime();
            long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (atLeastExpect != 0 && ret.size() < atLeastExpect && System.nanoTime() < deadline) {
                try {
                    LogEntry e;
                    if ((e = getApplied().poll(timeoutMs, TimeUnit.MILLISECONDS)) != null) {
                        ret.add(e);
                        getApplied().drainTo(ret);
                    }
                } catch (InterruptedException ex) {
                    // ignore
                }
            }

            return Collections.unmodifiableList(ret);
        }

        List<LogEntry> drainAvailableApplied() {
            List<LogEntry> ret = new ArrayList<>();

            getApplied().drainTo(ret);

            return Collections.unmodifiableList(ret);
        }
    }

    static class TestingBroker implements RaftCommandBroker {
        @Override
        public void onWriteCommand(RaftCommand cmd) {
            logger.debug("node {} write command {}", cmd.getFrom(), cmd.toString());
            String to = cmd.getTo();
            RaftNode toNode = nodes.get(to);
            if (toNode != null) {
                toNode.receiveCommand(cmd);
            }
        }
    }
}
