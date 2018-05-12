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
    private static final long defaultTimeoutMs = 5000;
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");
    private static final String persistentStateDir = "./target/deep/deep/deep/persistent";

    private final Map<String, RaftNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, TestingRaftStateMachine> stateMachines = new ConcurrentHashMap<>();
    private final Config.ConfigBuilder configBuilder = Config.newBuilder()
                    .withPersistentStateFileDirPath(persistentStateDir);
    private final TestingBroker broker = new TestingBroker();


    TestingRaftCluster(List<String> peers) {
        for (String peerId : peers) {
            RaftNode n = createTestingNode(peerId, peers);
            nodes.put(peerId, n);
        }
    }

    RaftNode createTestingNode(String selfId, Collection<String> peers) {
        TestingRaftStateMachine stateMachine = new TestingRaftStateMachine();
        stateMachines.put(selfId, stateMachine);

        Config c = configBuilder
                .withPeers(peers)
                .withSelfID(selfId)
                .withRaftCommandBroker(broker)
                .withStateMachine(stateMachine)
                .build();
        return new RaftNode(c);
    }

    void startCluster() {
        nodes.values().forEach(RaftNode::start);
    }

    void clearClusterPreviousPersistentState() {
        TestUtil.cleanDirectory(Paths.get(persistentStateDir));
    }

    void clearPreviousPersistentState(String peerId) {
        RaftPersistentState state = new RaftPersistentState(persistentStateDir, peerId);
        state.setTermAndVotedFor(0, null);
    }

    RaftNode waitLeaderElected() throws TimeoutException, InterruptedException{
        return this.waitLeaderElected(defaultTimeoutMs);
    }

    RaftNode waitLeaderElected(long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            for (RaftNode n : nodes.values()) {
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

    List<RaftNode> getFollowers() throws TimeoutException, InterruptedException{
        waitLeaderElected();
        ArrayList<RaftNode> followers = new ArrayList<>();
        for (Map.Entry<String, RaftNode> e : nodes.entrySet()) {
            RaftNode n = e.getValue();
            if (! n.isLeader()) {
                followers.add(e.getValue());
            }
        }
        return followers;
    }

    RaftNode getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    TestingRaftStateMachine getStateMachineById(String peerId) {
        TestingRaftStateMachine stateMachine = stateMachines.get(peerId);
        if (stateMachine != null) {
            return stateMachine;
        }
        throw new RuntimeException("no state machine for " + peerId);
    }

    void shutdownCluster(){
        for (RaftNode n : nodes.values()) {
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

    List<LogEntry> drainAvailableApplied(String peerId) {
        List<LogEntry> ret = new ArrayList<>();

        TestingRaftStateMachine stateMachine = getStateMachineById(peerId);
        stateMachine.getApplied().drainTo(ret);

        return Collections.unmodifiableList(ret);
    }

    List<LogEntry> waitApplied(String peerId, int atLeastExpect){
        return waitApplied(peerId, atLeastExpect, defaultTimeoutMs);
    }

    List<LogEntry> waitApplied(String peerId, int atLeastExpect, long timeoutMs) {
        Preconditions.checkArgument(atLeastExpect >= 0);

        List<LogEntry> ret = new ArrayList<>(atLeastExpect);
        TestingRaftStateMachine stateMachine = getStateMachineById(peerId);
        stateMachine.getApplied().drainTo(ret);

        long start = System.nanoTime();
        long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (atLeastExpect != 0 && ret.size() < atLeastExpect && System.nanoTime() < deadline) {
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

    void waitBecomeFollower(String peerId, long timeoutMs) throws TimeoutException, InterruptedException{
        long start = System.currentTimeMillis();
        while (true) {
            RaftNode node = getNodeById(peerId);
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

    class TestingRaftStateMachine implements StateMachine{
        private final BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();

        TestingRaftStateMachine(){
        }

        @Override
        public void onNodeAdded(String peerId) {
            RaftNode n = nodes.computeIfAbsent(peerId, k -> createTestingNode(peerId, nodes.keySet()));
            n.start();
        }

        @Override
        public void onNodeRemoved(String peerId) {
            shutdownPeer(peerId);
        }

        @Override
        public void onProposalCommitted(List<LogEntry> msgs) {
            assert msgs != null && ! msgs.isEmpty(): "msgs is null:" + (msgs == null);
            applied.addAll(msgs);
        }

        @Override
        public void onShutdown() {
            logger.info("state machine shutdown");
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
            RaftNode toNode = nodes.get(to);
            if (toNode != null) {
                toNode.receiveCommand(cmd);
            }
        }
    }
}
