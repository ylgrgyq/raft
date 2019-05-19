package raft.server;

import raft.server.util.ThreadFactoryImpl;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

final class DefaultTimeoutManager implements TimeoutManager {
    private final ScheduledExecutorService tickGenerator;
    private final long tickIntervalMs;
    private final AtomicLong electionTickCounter;
    private final AtomicBoolean electionTickerTimeout;
    private final AtomicLong pingTickCounter;
    private final AtomicBoolean pingTickerTimeout;
    private final long pingIntervalTicks;
    private final long suggestElectionTimeoutTicks;
    private final TimeoutListener onTimeout;

    // TODO initialize electionTimeoutTicks on start
    private long electionTimeoutTicks;

    DefaultTimeoutManager(RaftConfigurations config, TimeoutListener onTimeout) {
        this.electionTickCounter = new AtomicLong();
        this.electionTickerTimeout = new AtomicBoolean();
        this.pingTickCounter = new AtomicLong();
        this.pingTickerTimeout = new AtomicBoolean();
        this.tickGenerator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("tick-generator-"));
        this.tickIntervalMs = config.tickIntervalMs;
        this.pingIntervalTicks = config.pingIntervalTicks;
        this.suggestElectionTimeoutTicks = config.suggestElectionTimeoutTicks;
        this.onTimeout = onTimeout;
    }

    @Override
    public void start() {
        resetElectionTimeoutTicks();
        tickGenerator.scheduleWithFixedDelay(() -> {
            boolean electionTimeout = false;
            boolean pingTimeout = false;
            if (electionTickCounter.incrementAndGet() >= electionTimeoutTicks) {
                electionTickCounter.set(0L);
                electionTickerTimeout.compareAndSet(false, true);
                electionTimeout = true;
            }

            if (pingTickCounter.incrementAndGet() >= pingIntervalTicks) {
                pingTickCounter.set(0L);
                pingTickerTimeout.compareAndSet(false, true);
                pingTimeout = true;
            }

            if (electionTimeout || pingTimeout) {
                onTimeout.onTimeout(electionTimeout, pingTimeout);
            }

        }, tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isElectionTimeout() {
        return electionTickerTimeout.get();
    }

    @Override
    public long getElectionTimeoutTicks() {
        return electionTimeoutTicks;
    }

    @Override
    public void resetElectionTimeoutTicks() {
        // we need to reset election timeout on every time state changed and every
        // reelection in candidate state to avoid split vote
        long ticks = suggestElectionTimeoutTicks;
        electionTimeoutTicks = ticks + ThreadLocalRandom.current().nextLong(ticks);
    }

    @Override
    public void clearAllTimeoutMark() {
        electionTickerTimeout.getAndSet(false);
        pingTickerTimeout.getAndSet(false);
    }

    @Override
    public void clearElectionTickCounter() {
        electionTickCounter.set(0);
    }

    @Override
    public void clearAllTickCounters() {
        electionTickCounter.set(0);
        pingTickCounter.set(0);
    }

    @Override
    public void shutdown() {
        tickGenerator.shutdown();
    }
}
