package raft.server;

import com.google.common.base.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
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

    private Config(ConfigBuilder builder) {
        this.tickIntervalMs = builder.tickIntervalMs;
        this.pingIntervalTicks = builder.pingIntervalTicks;
        this.suggestElectionTimeoutTicks = builder.suggestElectionTimeoutTicks;
        this.maxEntriesPerAppend = builder.maxEntriesPerAppend;
        this.peers = builder.peers;
        this.selfId = builder.selfId;
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
        private String selfId;

        public ConfigBuilder withSelfID(String selfId) {
            this.selfId = Objects.requireNonNull(selfId, "selfId must not be null");
            return this;
        }

        public ConfigBuilder withPeers(List<String> peers) {
            this.peers = Objects.requireNonNull(peers, "peerId list must not be null");
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

        public Config build() {
            Preconditions.checkNotNull(selfId, "Must provide self Id");
            return new Config(this);
        }
    }


}
