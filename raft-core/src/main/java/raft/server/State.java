package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/12/19
 */
public enum State {
    UNINITIALIZED,
    INITIALIZING,
    LEADER,
    TRANSFERRING,
    CANDIDATE,
    FOLLOWER,
    ERROR,
    SHUTTING_DOWN,
    SHUTDOWN;

    public boolean isActive() {
        return ordinal() > INITIALIZING.ordinal() && ordinal() < ERROR.ordinal();
    }

    public boolean isShuttingDown() {
        return this.ordinal() >= SHUTTING_DOWN.ordinal();
    }
}
