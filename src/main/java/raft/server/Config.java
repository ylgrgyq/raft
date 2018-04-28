package raft.server;

import com.google.common.base.Preconditions;

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

    private Config(ConfigBuilder builder) {
        this.tickIntervalMs = builder.tickIntervalMs;
        this.pingIntervalTicks = builder.pingIntervalTicks;
        this.suggestElectionTimeoutTicks = builder.suggestElectionTimeoutTicks;
        this.maxEntriesPerAppend = builder.maxEntriesPerAppend;
        this.peers = builder.peers;
        this.selfId = builder.selfId;
        this.persistentStateFileDirPath = builder.persistentStateFileDirPath;
    }

    public static ConfigBuilder newBuilder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        private long tickIntervalMs = 10;
        private long pingIntervalTicks = 20;
        private long suggestElectionTimeoutTicks = 60;
        private int maxEntriesPerAppend = 16;
        private String persistentStateFileDirPath;

        private List<String> peers = Collections.emptyList();
        private String selfId;

        public ConfigBuilder withSelfID(String selfId) {
            this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
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
            Preconditions.checkNotNull(path);
            this.persistentStateFileDirPath = path;
            return this;
        }

        public Config build() {
            Preconditions.checkNotNull(selfId, "Must provide self Id");
            Preconditions.checkNotNull(persistentStateFileDirPath, "Must provide directory path to save raft persistent state");
            return new Config(this);
        }
    }


}
