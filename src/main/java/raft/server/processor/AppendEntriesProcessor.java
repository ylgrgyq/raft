package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
//public class AppendEntriesProcessor extends AbstractServerCmdProcessor<AppendEntriesCommand> {
//    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());
//
//    public AppendEntriesProcessor(RaftServer server) {
//        super(server);
//    }
//
//    @Override
//    protected AppendEntriesCommand decodeRemotingCommand(byte[] requestBody) {
//        return new AppendEntriesCommand(requestBody);
//    }
//
//    @Override
//    protected RemotingCommand process0(AppendEntriesCommand req) {
//        logger.debug("receive directAppend entries command, request={}, server={}", req, this.getServer());
//        final int termInEntry = req.getTerm();
//        final RaftServer server = this.getServer();
//        int termInServer = server.getTerm();
//
//        final AppendEntriesCommand response = new AppendEntriesCommand(termInServer, server.getId());
//        response.setSuccess(false);
//
//        if (termInEntry >= termInServer) {
//            boolean success = server.replicateLogsOnFollower(req.getPrevLogIndex(), req.getPrevLogTerm(), req.getLeaderCommit(), req.getFrom(), req.getEntries());
//            response.setSuccess(success);
//        }
//
//        logger.debug("respond directAppend entries command, response={}, server={}", response, this.getServer());
//        return RemotingCommand.createResponseCommand(response);
//    }
//}
