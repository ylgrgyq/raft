package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.storage.FileBasedStorage;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");

    private final RaftConfigurations.ConfigBuilder configBuilder;
    private final Map<String, Raft> nodes;
    private final Map<String, TestingRaftStateMachine> stateMachines;
    private final TestingBroker broker;
    private final String storageName;
    private final String persistentStateDir;
    private final String storageDir;

    public TestingRaftCluster(String testingName) {
        this.nodes = new ConcurrentHashMap<>();
        this.stateMachines = new ConcurrentHashMap<>();
        this.broker = new TestingBroker(nodes, logger);
        this.persistentStateDir = Paths.get(TestingConfigs.persistentStateDir, testingName).toString();
        this.storageDir = Paths.get(TestingConfigs.testingStorageDirectory, testingName).toString();
        this.storageName = testingName + "_" + "LogStorage";
        this.configBuilder = RaftConfigurations.newBuilder()
                .withPersistentMetaFileDirPath(this.persistentStateDir);
    }

    Raft addTestingNode(String peerId, Collection<String> peers) {
        return nodes.computeIfAbsent(peerId, k -> createTestingNode(peerId, peers));
    }

    private Raft createTestingNode(String peerId, Collection<String> peers) {
        FileBasedStorage storage = new FileBasedStorage(storageDir, getStorageName(peerId));
        TestingRaftStateMachine stateMachine = new TestingRaftStateMachine(logger, peerId, peers, storage);
        stateMachines.put(peerId, stateMachine);

        RaftConfigurations c = configBuilder
                .withPeers(peers)
                .withSelfID(peerId)
                .withRaftCommandBroker(broker)
                .withStateMachine(stateMachine)
                .withPersistentMetaFileDirPath(persistentStateDir)
                .withPersistentStorage(storage)
                .build();
        return new RaftImpl(c);
    }

    private String getStorageName(String peerId) {
        return storageName + "_" + peerId.replace(" ", "");
    }

    void startCluster(Collection<String> peers) {
        for (String peerId : peers) {
            Raft node = createTestingNode(peerId, peers);
            this.nodes.put(peerId, node);
            node.start();
        }
    }

    TestingRaftStateMachine waitGetLeader() throws TimeoutException, InterruptedException {
        return waitGetLeader(TestingConfigs.defaultTimeoutMs);
    }

    TestingRaftStateMachine waitGetLeader(long timeoutMs) throws TimeoutException, InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            ArrayList<TestingRaftStateMachine> candidates = new ArrayList<>(stateMachines.size());
            for (TestingRaftStateMachine n : stateMachines.values()) {
                if (n.getLastStatus().isLeader()) {
                    candidates.add(n);
                }
            }

            if (candidates.size() != 0) {
                if (candidates.size() == 1) {
                    return candidates.get(0);
                }

                // we could have more than one leader but they must have different term.
                // the scenario is we have A B C three nodes, A was the old leader and now B C elect B as new leader.
                // Before B had a chance to notify A it is the new leader, A continue to consider itself as leader. At
                // this moment we have two leader A and B but they don't share the same term. B's term must
                // be greater than A's.
                logger.warn("we have more than one leader: {}", candidates);
            }

            if (timeoutMs != 0 && (System.currentTimeMillis() - start > timeoutMs)) {
                throw new TimeoutException();
            } else {
                Thread.sleep(200);
            }
        }
    }

    List<TestingRaftStateMachine> getFollowers() throws TimeoutException, InterruptedException, ExecutionException {
        TestingRaftStateMachine leader = waitGetLeader();
        List<TestingRaftStateMachine> followers = stateMachines.values()
                .stream()
                .filter(n -> !n.getId().equals(leader.getId()))
                .collect(Collectors.toList());

        for (TestingRaftStateMachine follower : followers) {
            follower.becomeFollowerFuture().get(3, TimeUnit.SECONDS);
        }
        return followers;
    }

    List<TestingRaftStateMachine> getAllStateMachines() {
        return new ArrayList<>(stateMachines.values());
    }

    Raft getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    Collection<String> getAllPeerIds() {
        return nodes.keySet();
    }

    TestingRaftStateMachine getStateMachineById(String peerId) {
        TestingRaftStateMachine stateMachine = stateMachines.get(peerId);
        if (stateMachine != null) {
            return stateMachine;
        }
        throw new RuntimeException("no state machine for " + peerId);
    }

    void shutdownCluster() throws InterruptedException{
        for (Raft n : nodes.values()) {
            n.shudownGracefully();
        }
        nodes.clear();
        stateMachines.clear();
    }

    void shutdownPeer(String peerId) throws InterruptedException{
        stateMachines.remove(peerId);
        Raft n = nodes.remove(peerId);
        n.shudownGracefully();
    }

    void clearPersistentState() {
        TestUtil.cleanDirectory(Paths.get(this.persistentStateDir));
    }

    void clearPersistentStateFor(String peerId) {
        LocalFileRaftPersistentMeta state = new LocalFileRaftPersistentMeta(persistentStateDir, peerId, false);
        state.setTermAndVotedFor(0, null);
    }

    void clearLogStorage() {
        TestUtil.cleanDirectory(Paths.get(storageDir));
    }

    void clearLogStorageFor(String peerId) {
        TestUtil.cleanDirectory(Paths.get(storageDir, getStorageName(peerId)));
    }
}
