package raft.server;

import raft.server.proto.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftState implements LifeCycle<RaftImpl.Context, RaftImpl.Context> {
    private final State state;

    RaftState(State state){
        this.state = state;
    }

    State getState() {
        return state;
    }

    abstract void process(RaftCommand cmd);

    public void onElectionTimeout() {}

    public void onPingTimeout() {}
}
