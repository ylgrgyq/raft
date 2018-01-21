package raft.server;

import raft.server.processor.RaftCommandListener;
import raft.server.rpc.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftState<T extends RaftCommand> implements LifeCycle, RaftCommandListener<T>, RaftServer.TickTimeoutProcessor {
    private final State state;
    protected final RaftServer server;

    RaftState(RaftServer server, State state){
        this.state = state;
        this.server = server;
    }

    State getState() {
        return state;
    }

    @Override
    public void onReceiveRaftCommand(T cmd) {}
}
