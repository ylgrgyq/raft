package raft.server;

import raft.server.processor.RaftServerCommandListener;
import raft.server.rpc.RaftServerCommand;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftState<T extends RaftServerCommand> implements LifeCycle, RaftServerCommandListener<T> {
    protected final ScheduledExecutorService timer;

    private final State state;
    protected final RaftServer server;

    RaftState(RaftServer server, ScheduledExecutorService timer, State state){
        this.state = state;
        this.server = server;
        this.timer = timer;
    }

    State getState() {
        return state;
    }

    @Override
    public void onReceiveRaftServerCommand(T cmd) {}
}
