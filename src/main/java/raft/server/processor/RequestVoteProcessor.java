package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

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
        RequestVoteCommand vote = new RequestVoteCommand();
        vote.decode(request.getBody());
        System.out.println("Receive msg: " + vote);
        return null;
    }
}
