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
        RequestVoteCommand res;
        final int termInVote = vote.getTerm();
        final int termInServer = this.server.getTerm();
        if (termInVote > termInServer &&
                server.tryTransitStateToFollower(termInVote, vote.getCandidateId())) {
            res = new RequestVoteCommand(termInVote);
            res.setVoteGranted(true);
        } else {
            res = new RequestVoteCommand(termInServer);
            res.setVoteGranted(false);
        }

        return RemotingCommand.createResponseCommand(res);
    }
}
