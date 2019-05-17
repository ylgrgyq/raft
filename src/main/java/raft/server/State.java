package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/12/19
 */
public enum State {
    LEADER,
    TRANSFERRING,
    CANDIDATE,
    FOLLOWER,
    ERROR,
    UNINITIALIZED,
    SHUTTING,
    SHUTDOWN,
    END;

    public boolean isActive() {
        return this.ordinal() < ERROR.ordinal();
    }

}
