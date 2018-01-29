package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftLog;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractServerCmdProcessor<AppendEntriesCommand> {
    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());

    public AppendEntriesProcessor(RaftServer server) {
        super(server);
    }

    @Override
    protected AppendEntriesCommand decodeRemotingCommand(byte[] requestBody) {
        return new AppendEntriesCommand(requestBody);
    }

    @Override
    protected RemotingCommand process0(AppendEntriesCommand req) {
        logger.debug("receive append entries command, request={}, server={}", req, this.getServer());
        final int termInEntry = req.getTerm();
        final RaftServer server = this.getServer();
        final RaftLog raftLog = server.getRaftLog();
        int termInServer = server.getTerm();

        final AppendEntriesCommand response = new AppendEntriesCommand(termInServer, server.getId());
        response.setSuccess(false);

        raftLog.getEntry(req.getPrevLogIndex()).ifPresent(prevEntry -> {
            if (prevEntry.getTerm() == req.getPrevLogTerm() && termInEntry >= termInServer) {
                assert termInEntry == termInServer;

                try {
                    server.setLeaderId(req.getFrom());
                    raftLog.appendEntries(req.getEntries());
                    response.setSuccess(true);
                    raftLog.tryCommitTo(req.getLeaderCommit());
                } catch (Exception ex) {
                    logger.error("append entries failed, request={}", req, ex);
                }
            }
        });

        logger.debug("respond append entries command, response={}, server={}", response, this.getServer());
        return RemotingCommand.createResponseCommand(response);
    }
}
