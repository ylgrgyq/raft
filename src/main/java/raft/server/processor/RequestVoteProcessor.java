package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class RequestVoteProcessor extends AbstractProcessor<RequestVoteCommand> {
    private static final Logger logger = LoggerFactory.getLogger(RequestVoteProcessor.class.getName());

    public RequestVoteProcessor(RaftServer server) {
        this(server, null);
    }

    public RequestVoteProcessor(RaftServer server, List<RaftServerCommandListener<RequestVoteCommand>> listeners) {
        super(server, listeners);
    }

    @Override
    protected RequestVoteCommand decodeRemotingCommand(RemotingCommand request) {
        return new RequestVoteCommand(request.getBody());
    }

    @Override
    protected RemotingCommand doProcess(RequestVoteCommand vote) {
        logger.debug("receive request vote command, cmd={}, server={}", vote, this.server);
        RequestVoteCommand res;
        final int termInVote = vote.getTerm();
        final int termInServer = this.server.getTerm();
        if (termInVote > termInServer) {
            res = new RequestVoteCommand(termInVote);
            res.setVoteGranted(true);
            res.setCandidateId(vote.getCandidateId());
        } else {
            res = new RequestVoteCommand(termInServer);
            res.setVoteGranted(false);
            res.setCandidateId(vote.getCandidateId());
        }

        logger.warn("respond request vote command, response={}, server={}", res, this.server);
        return RemotingCommand.createResponseCommand(res);
    }
}
