package raft.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import raft.server.log.MemoryFakePersistentStorage;
import raft.server.log.PersistentStorage;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public class Config {
    final long tickIntervalMs;
    final long pingIntervalTicks;
    final long suggestElectionTimeoutTicks;
    final int maxEntriesPerAppend;

    final List<String> peers;
    final String selfId;
    final String persistentStateFileDirPath;
    final StateMachine stateMachine;
    final RaftCommandBroker broker;
    final PersistentStorage storage;

    private Config(ConfigBuilder builder) {
        this.tickIntervalMs = builder.tickIntervalMs;
        this.pingIntervalTicks = builder.pingIntervalTicks;
        this.suggestElectionTimeoutTicks = builder.suggestElectionTimeoutTicks;
        this.maxEntriesPerAppend = builder.maxEntriesPerAppend;
        this.peers = builder.peers;
        this.selfId = builder.selfId;
        this.persistentStateFileDirPath = builder.persistentStateFileDirPath;
        this.stateMachine = builder.stateMachine;
        this.broker = builder.broker;
        this.storage = builder.storage;
    }

    public static ConfigBuilder newBuilder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private long tickIntervalMs = 10;
        private long pingIntervalTicks = 20;
        private long suggestElectionTimeoutTicks = 60;
        private int maxEntriesPerAppend = 16;
        private List<String> peers = Collections.emptyList();

        private StateMachine stateMachine;
        private RaftCommandBroker broker;
        private String persistentStateFileDirPath;
        private String selfId;
        private PersistentStorage storage;

        public ConfigBuilder withSelfID(String selfId) {
            this.selfId = selfId;
            return this;
        }

        public ConfigBuilder withPeers(Collection<String> peers) {
            peers = Objects.requireNonNull(peers, "peers must not be null");
            Preconditions.checkArgument(peers instanceof Set || peers.size() == new HashSet<>(peers).size(),
                    "found duplicate id in peers: %s", peers);
            this.peers = new ArrayList<>(peers);
            return this;
        }

        public ConfigBuilder withTickInterval(long interval, TimeUnit unit) {
            Preconditions.checkArgument(interval > 0, "tick interval must be positive, actual %s", interval);
            this.tickIntervalMs = unit.toMillis(interval);
            return this;
        }

        public ConfigBuilder withPingIntervalTicks(long ticks) {
            Preconditions.checkArgument(ticks > 0, "ping interval ticks must be positive, actual %s", ticks);
            this.pingIntervalTicks = ticks;
            return this;
        }

        public ConfigBuilder withSuggestElectionTimeoutTicks(long ticks) {
            Preconditions.checkArgument(ticks > 0, "suggest election timeout ticks must be positive, actual %s", ticks);
            this.suggestElectionTimeoutTicks = ticks;
            return this;
        }

        public ConfigBuilder withPersistentStateFileDirPath(String path) {
            this.persistentStateFileDirPath = path;
            return this;
        }

        public ConfigBuilder withStateMachine(StateMachine stateMachine) {
            this.stateMachine = stateMachine;
            return this;
        }

        public ConfigBuilder withRaftCommandBroker(RaftCommandBroker broker) {
            this.broker = broker;
            return this;
        }

        public ConfigBuilder withPersistentStorage(PersistentStorage storage) {
            this.storage = storage;
            return this;
        }

        public Config build() {
            Preconditions.checkArgument(! Strings.isNullOrEmpty(selfId), "Must provide non-empty self Id");
            Preconditions.checkArgument(! Strings.isNullOrEmpty(persistentStateFileDirPath),
                    "Must provide a non-empty directory path to save raft persistent state");
            Preconditions.checkNotNull(stateMachine, "Must provide a state machine implementation");
            Preconditions.checkNotNull(broker, "Must provide a broker to transfer raft command between raft nodes");

            if (peers.stream().noneMatch(p -> p.equals(selfId))) {
                peers.add(selfId);
            }

            if (storage == null) {
                storage = new MemoryFakePersistentStorage();
            }

            return new Config(this);
        }
    }


}
