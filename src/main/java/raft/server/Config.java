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
    long tickIntervalMs;
    long pingIntervalTicks;
    long suggestElectionTimeoutTicks;
    int maxMsgSize;

    List<String> peers;
    String selfId;

    private Config() {}

    public static ConfigBuilder newBuilder() {
        return new ConfigBuilder();
    }

    public static class ConfigBuilder {
        long tickIntervalMs = 10;
        long pingIntervalTicks = 20;
        long suggestElectionTimeoutTicks = 40;
        int maxMsgSize = 16;

        List<String> peers = Collections.emptyList();
        String selfId;

        public ConfigBuilder withSelfID(String selfID) {
            this.selfId = selfID;
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
            Config c = new Config();
            c.selfId = Objects.requireNonNull(selfId, "Must provide self Id");
            c.tickIntervalMs = tickIntervalMs;
            c.pingIntervalTicks = pingIntervalTicks;
            c.suggestElectionTimeoutTicks = suggestElectionTimeoutTicks;
            c.maxMsgSize = maxMsgSize;
            c.peers = peers;

            return c;
        }
    }


}
