package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftLog;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RaftCommand;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractServerCmdProcessor<AppendEntriesCommand> {
    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());

    public AppendEntriesProcessor(RaftServer server) {
        this(server, Collections.emptyList());
    }

    public AppendEntriesProcessor(RaftServer server, List<RaftCommandListener<RaftCommand>> listeners) {
        super(server, listeners);
    }

    @Override
    protected AppendEntriesCommand decodeRemotingCommand(RemotingCommand request) {
        return new AppendEntriesCommand(request.getBody());
    }

    @Override
    protected RemotingCommand process0(AppendEntriesCommand appendCmd) {
        logger.debug("receive append entries command, cmd={}, server={}", appendCmd, this.getServer());
        final int termInEntry = appendCmd.getTerm();
        final RaftServer server = this.getServer();
        final RaftLog raftLog = server.getRaftLog();
        int termInServer = server.getTerm();

        final AppendEntriesCommand response = new AppendEntriesCommand(termInServer, server.getId());
        response.setSuccess(false);

        raftLog.getEntry(appendCmd.getPrevLogIndex()).ifPresent(prevEntry -> {
            if (prevEntry.getTerm() == appendCmd.getPrevLogTerm() && termInEntry >= termInServer) {
                assert termInEntry == termInServer;

                try {
                    server.setLeaderId(appendCmd.getFrom());
                    raftLog.appendEntries(appendCmd.getEntries());
                    response.setSuccess(true);
                    raftLog.tryCommitTo(appendCmd.getLeaderCommit());
                } catch (Exception ex) {
                    logger.error("append entries failed, cmd={}", appendCmd, ex);
                }
            }
        });

        logger.debug("respond append entries command, response={}, server={}", response, this.getServer());
        return RemotingCommand.createResponseCommand(response);
    }
}
