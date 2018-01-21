package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftState implements LifeCycle, TickTimeoutProcessor {
    private final State state;

    RaftState(State state){
        this.state = state;
    }

    State getState() {
        return state;
    }
}
