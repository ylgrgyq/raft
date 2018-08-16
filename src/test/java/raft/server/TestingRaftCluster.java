package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.storage.FileBasedStorage;

import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;

/**
 * Author: ylgrgyq
 * Date: 18/3/27
 */
class TestingRaftCluster {
    private static final Logger logger = LoggerFactory.getLogger("TestingRaftCluster");

    private final Config.ConfigBuilder configBuilder;
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
        this.configBuilder = Config.newBuilder()
                .withPersistentMetaFileDirPath(this.persistentStateDir);
    }

    Raft addTestingNode(String peerId, Collection<String> peers) {
        return nodes.computeIfAbsent(peerId, k -> createTestingNode(peerId, peers));
    }

    private Raft createTestingNode(String peerId, Collection<String> peers) {
        TestingRaftStateMachine stateMachine = new TestingRaftStateMachine(logger, peers);
        stateMachines.put(peerId, stateMachine);

        Config c = configBuilder
                .withPeers(peers)
                .withSelfID(peerId)
                .withRaftCommandBroker(broker)
                .withStateMachine(stateMachine)
                .withPersistentMetaFileDirPath(persistentStateDir)
                .withPersistentStorage(new FileBasedStorage(storageDir, getStorageName(peerId)))
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

    Raft waitGetLeader() throws TimeoutException, InterruptedException {
        return waitGetLeader(TestingConfigs.defaultTimeoutMs);
    }

    Raft waitGetLeader(long timeoutMs) throws TimeoutException, InterruptedException {
        long start = System.currentTimeMillis();
        while (true) {
            ArrayList<Raft> possibleLeaderNodes = new ArrayList<>(nodes.size());
            for (Raft n : nodes.values()) {
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

    List<Raft> getFollowers() throws TimeoutException, InterruptedException {
        waitGetLeader();
        ArrayList<Raft> followers = new ArrayList<>();
        for (Map.Entry<String, Raft> e : nodes.entrySet()) {
            Raft n = e.getValue();
            if (!n.isLeader()) {
                followers.add(e.getValue());
            }
        }
        return followers;
    }

    Raft getNodeById(String peerId) {
        return nodes.get(peerId);
    }

    TestingRaftStateMachine getStateMachineById(String peerId) {
        TestingRaftStateMachine stateMachine = stateMachines.get(peerId);
        if (stateMachine != null) {
            return stateMachine;
        }
        throw new RuntimeException("no state machine for " + peerId);
    }

    void shutdownCluster() {
        for (Raft n : nodes.values()) {
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

    void clearPersistentState() {
        TestUtil.cleanDirectory(Paths.get(this.persistentStateDir));
    }

    void clearPersistentStateFor(String peerId) {
        RaftPersistentMeta state = new RaftPersistentMeta(persistentStateDir, peerId, false);
        state.setTermAndVotedFor(0, null);
    }

    void clearLogStorage() {
        TestUtil.cleanDirectory(Paths.get(storageDir));
    }

    void clearLogStorageFor(String peerId) {
        TestUtil.cleanDirectory(Paths.get(storageDir, getStorageName(peerId)));
    }
}
