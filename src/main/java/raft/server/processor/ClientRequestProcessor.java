package raft.server.processor;

import raft.server.LogEntry;
import raft.server.RaftServer;
import raft.server.State;
import raft.server.rpc.RaftClientCommand;
import raft.server.rpc.RemotingCommand;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public class ClientRequestProcessor extends AbstractProcessor<RaftClientCommand> {
    public ClientRequestProcessor(RaftServer server) {
        super(server);
    }

    @Override
    protected RaftClientCommand decodeRemotingCommand(byte[] requestBody) {
        return new RaftClientCommand(requestBody);
    }

    @Override
    protected RemotingCommand doProcess(RaftClientCommand cmd) {
        RaftClientCommand res = new RaftClientCommand();
        res.setLeaderId(this.getServer().getLeaderId());
        if (this.getServer().getState() == State.LEADER) {
            LogEntry body = cmd.getEntry();
            // TODO Handle append log failed
            this.getServer().appendLog(body);

            res.setSuccess(true);
        } else {
            res.setSuccess(false);
        }

        return RemotingCommand.createResponseCommand(res);
    }
}
