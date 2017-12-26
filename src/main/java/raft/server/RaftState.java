package raft.server;

import raft.server.processor.RaftCommandListener;
import raft.server.rpc.RaftServerCommand;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftState<T extends RaftServerCommand> implements LifeCycle, RaftCommandListener<T> {
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
    public void onReceiveRaftCommand(T cmd) {}
}
