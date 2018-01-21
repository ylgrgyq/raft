package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/1/17
 */
public abstract class AbstractServerCmdProcessor<T extends RaftServerCommand> extends AbstractProcessor<T> {
    private List<RaftCommandListener<RaftServerCommand>> raftCommandListeners = Collections.emptyList();

    AbstractServerCmdProcessor(RaftServer server, List<RaftCommandListener<RaftServerCommand>> listeners) {
        super(server);

        if (listeners != null) {
            this.raftCommandListeners = listeners;
        } else {
            this.raftCommandListeners = new ArrayList<>();
        }
    }

    protected abstract RemotingCommand process0(T appendCmd);

    public RemotingCommand doProcess(T cmd) {
        this.raftCommandListeners.forEach(listener -> listener.onReceiveRaftCommand(cmd));

        int termInServer = this.getServer().getTerm();
        int termInCmd = cmd.getTerm();

        if (termInCmd > termInServer) {
            this.getServer().tryBecomeFollower(termInCmd, cmd.getFrom());
        }

        return this.process0(cmd);
    }
}
