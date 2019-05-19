package raft.server;

public interface TimeoutManager extends LifeCycle {
    boolean isElectionTimeout();

    long getElectionTimeoutTicks();

    void resetElectionTimeoutTicks();

    void clearAllTimeoutMark();

    void clearElectionTickCounter();

    void clearAllTickCounters();
}
