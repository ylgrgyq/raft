package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 18/1/17
 */
public abstract class AbstractServerCmdProcessor<T extends RaftServerCommand> extends AbstractProcessor<T> {
    AbstractServerCmdProcessor(RaftServer server) {
        super(server);
    }

    protected abstract RemotingCommand process0(T appendCmd);

    public RemotingCommand doProcess(T cmd) {
        this.getServer().onReceiveRaftCommand(cmd);

        int termInServer = this.getServer().getTerm();
        int termInCmd = cmd.getTerm();

        if (termInCmd > termInServer) {
            this.getServer().tryBecomeFollower(termInCmd, cmd.getFrom());
        }

        return this.process0(cmd);
    }
}
