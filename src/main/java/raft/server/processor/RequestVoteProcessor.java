package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.LogEntry;
import raft.server.RaftLog;
import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class RequestVoteProcessor extends AbstractServerCmdProcessor<RequestVoteCommand> {
    private static final Logger logger = LoggerFactory.getLogger(RequestVoteProcessor.class.getName());

    public RequestVoteProcessor(RaftServer server) {
        this(server, null);
    }

    public RequestVoteProcessor(RaftServer server, List<RaftCommandListener<RequestVoteCommand>> listeners) {
        super(server, listeners);
    }

    @Override
    protected RequestVoteCommand decodeRemotingCommand(RemotingCommand request) {
        return new RequestVoteCommand(request.getBody());
    }

    @Override
    protected RemotingCommand process0(RequestVoteCommand vote) {
        logger.debug("receive request vote command, cmd={}, server={}", vote, this.getServer());
        final int termInVote = vote.getTerm();
        final RaftServer server = this.getServer();
        final int termInServer = server.getTerm();
        final String candidateId = vote.getFrom();
        final String voteFor = server.getVoteFor();
        final RaftLog raftLog = server.getRaftLog();

        final RequestVoteCommand res = new RequestVoteCommand(termInServer, server.getId());
        res.setVoteGranted(false);

        if ((voteFor == null || voteFor.equals(candidateId)) &&
                termInVote >= termInServer &&
                raftLog.isUpToDate(vote.getLastLogTerm(), vote.getLastLogIndex())) {
            server.setVoteFor(candidateId);
            res.setVoteGranted(true);
        }

        logger.warn("respond request vote command, response={}, server={}", res, this.getServer());
        return RemotingCommand.createResponseCommand(res);
    }
}
