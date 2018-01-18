package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/1/17
 */
public abstract class AbstractServerCmdProcessor<T extends RaftServerCommand> extends AbstractProcessor<T> {
    AbstractServerCmdProcessor(RaftServer server, List<RaftCommandListener<T>> raftCommandListeners) {
        super(server, raftCommandListeners);
    }

    abstract RemotingCommand process0(T cmd);

    @Override
    protected RemotingCommand doProcess(T cmd) {
        int termInServer = this.getServer().getTerm();
        int termInCmd = cmd.getTerm();

        if (termInCmd > termInServer) {
            this.getServer().tryBecomeFollower(termInCmd, cmd.getFrom());
        }
        return process0(cmd);
    }
}
