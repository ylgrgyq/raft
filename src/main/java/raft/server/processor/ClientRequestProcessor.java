package raft.server.processor;

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
        this(server, Collections.emptyList());
    }

    public ClientRequestProcessor(RaftServer server, List<RaftCommandListener<RaftClientCommand>> listeners) {
        super(server, listeners);
    }

    @Override
    protected RaftClientCommand decodeRemotingCommand(RemotingCommand request) {
        return new RaftClientCommand(request.getBody());
    }

    @Override
    protected RemotingCommand doProcess(RaftClientCommand cmd) {
        RaftClientCommand res = new RaftClientCommand();
        res.setLeaderId(this.server.getLeaderId());
        if (this.server.getState() == State.LEADER) {
            byte[] body = cmd.getRequestBody();
            this.server.writeLog(body);

            res.setSuccess(true);
        } else {
            res.setSuccess(false);
        }

        return RemotingCommand.createResponseCommand(res);
    }
}
