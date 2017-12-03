package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class RequestVoteProcessor extends AbstractProcessor {

    public RequestVoteProcessor(RaftServer server) {
        super(server);
    }

    @Override
    public RemotingCommand processRequest(RemotingCommand request) {
        return null;
    }
}
