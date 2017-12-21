package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
abstract class AbstractProcessor<T extends RaftServerCommand> implements Processor{
    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());

    protected final RaftServer server;

    private List<RaftServerCommandListener<T>> raftServerCommandListeners = Collections.emptyList();

    AbstractProcessor(RaftServer server, List<RaftServerCommandListener<T>> listeners){
        this.server = server;
        if (listeners != null) {
            this.raftServerCommandListeners = listeners;
        } else {
            this.raftServerCommandListeners = new ArrayList<>();
        }
    }

    protected abstract T decodeRemotingCommand(RemotingCommand request);

    protected abstract RemotingCommand doProcess(T cmd);

    public RemotingCommand processRequest(RemotingCommand request) {
        T cmd = this.decodeRemotingCommand(request);

        this.raftServerCommandListeners.forEach(listener -> listener.onReceiveRaftServerCommand(cmd));
        return this.doProcess(cmd);
    }

    protected RaftServer getServer() {
        return server;
    }
}
