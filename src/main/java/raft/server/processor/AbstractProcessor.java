package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RaftCommand;
import raft.server.rpc.RemotingCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
abstract class AbstractProcessor<T extends RaftCommand> implements Processor{
    protected final RaftServer server;

    private List<RaftCommandListener<T>> raftCommandListeners = Collections.emptyList();

    AbstractProcessor(RaftServer server, List<RaftCommandListener<T>> listeners){
        this.server = server;
        if (listeners != null) {
            this.raftCommandListeners = listeners;
        } else {
            this.raftCommandListeners = new ArrayList<>();
        }
    }

    protected abstract T decodeRemotingCommand(RemotingCommand request);

    protected abstract RemotingCommand doProcess(T cmd);

    public RemotingCommand processRequest(RemotingCommand request) {
        T cmd = this.decodeRemotingCommand(request);

        this.raftCommandListeners.forEach(listener -> listener.onReceiveRaftCommand(cmd));
        return this.doProcess(cmd);
    }

    protected RaftServer getServer() {
        return server;
    }
}
