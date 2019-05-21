package raft.server;

import com.google.common.base.Strings;
import raft.server.log.PersistentStorage;
import raft.server.proto.RaftCommand;
import raft.server.util.Preconditions;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public class RaftConfigurations {
    final long tickIntervalMs;
    final long pingIntervalTicks;
    final long suggestElectionTimeoutTicks;
    final int maxEntriesPerAppend;
    final long appliedTo;
    final boolean syncWriteStateFile;

    final List<String> peers;
    final String selfId;
    final String persistentMetaFileDirPath;
    final StateMachine stateMachine;
    final RaftCommandBroker broker;
    final PersistentStorage storage;
    final BlockingQueue<RaftCommand> outputQueue;

    private RaftConfigurations(ConfigBuilder builder) {
        this.tickIntervalMs = builder.tickIntervalMs;
        this.pingIntervalTicks = builder.pingIntervalTicks;
        this.suggestElectionTimeoutTicks = builder.suggestElectionTimeoutTicks;
        this.maxEntriesPerAppend = builder.maxEntriesPerAppend;
        this.peers = builder.peers;
        this.selfId = builder.selfId;
        this.persistentMetaFileDirPath = builder.persistentMetaFileDirPath;
        this.stateMachine = builder.stateMachine;
        this.broker = builder.broker;
        this.storage = builder.storage;
        this.appliedTo = builder.appliedTo;
        this.syncWriteStateFile = builder.syncWriteStateFile;
        this.outputQueue = builder.outputQueue;
    }

    public static ConfigBuilder newBuilder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private long tickIntervalMs = 10;
        private long pingIntervalTicks = 20;
        private long suggestElectionTimeoutTicks = 60;
        private int maxEntriesPerAppend = 16;
        private long appliedTo = -1L;
        private List<String> peers = Collections.emptyList();
        private boolean syncWriteStateFile = false;

        private StateMachine stateMachine;
        private RaftCommandBroker broker;
        private String persistentMetaFileDirPath;
        private String selfId;
        private PersistentStorage storage;
        private BlockingQueue<RaftCommand> outputQueue;

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

        public ConfigBuilder withPersistentMetaFileDirPath(String path) {
            this.persistentMetaFileDirPath = path;
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

        public ConfigBuilder withRaftCommandOutputQueue(BlockingQueue<RaftCommand> outputQueue) {
            this.outputQueue = outputQueue;
            return this;
        }

        public ConfigBuilder withPersistentStorage(PersistentStorage storage) {
            this.storage = storage;
            return this;
        }

        public ConfigBuilder withAppliedTo(long appliedTo) {
            this.appliedTo = appliedTo;
            return this;
        }

        public ConfigBuilder withSyncWriteRaftStateFile(boolean syncWriteStateFile) {
            this.syncWriteStateFile = syncWriteStateFile;
            return this;
        }

        public RaftConfigurations build() {
            Preconditions.checkArgument(! Strings.isNullOrEmpty(selfId), "Must provide non-empty self Id");
            Preconditions.checkArgument(! Strings.isNullOrEmpty(persistentMetaFileDirPath),
                    "Must provide a non-empty directory path to save raft persistent state");
            Preconditions.checkNotNull(stateMachine, "Must provide a state machine implementation");
            Preconditions.checkNotNull(broker, "Must provide a broker to transfer raft command between raft nodes");
//            Preconditions.checkNotNull(outputQueue, "Must provide a output queue to transfer raft command between raft nodes");
            Preconditions.checkNotNull(storage, "Must provide a storage to save persistent logs");

            if (peers.stream().noneMatch(p -> p.equals(selfId))) {
                peers.add(selfId);
            }

            return new RaftConfigurations(this);
        }
    }


}
