package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class RequestVoteProcessor extends AbstractProcessor {
    private Logger logger = LoggerFactory.getLogger(RequestVoteProcessor.class.getName());

    public RequestVoteProcessor(RaftServer server) {
        super(server);
    }

    @Override
    public RemotingCommand processRequest(RemotingCommand request) {
        final RequestVoteCommand vote = new RequestVoteCommand(request.getBody());
        logger.info("Receive msg: " + vote);

        RequestVoteCommand res;
        final int termInVote = vote.getTerm();
        final int termInServer = this.server.getTerm();
        if (termInVote > termInServer) {
            server.checkTermThenTransferStateToFollower(termInVote, vote.getCandidateId());

            res = new RequestVoteCommand(termInVote);
            res.setVoteGranted(true);
        } else {
            res = new RequestVoteCommand(termInServer);
            res.setVoteGranted(false);
        }

        return RemotingCommand.createResponseCommand(res);
    }
}
