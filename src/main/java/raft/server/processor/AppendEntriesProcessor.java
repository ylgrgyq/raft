package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.LogEntry;
import raft.server.RaftLog;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractProcessor<AppendEntriesCommand> {
    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());

    public AppendEntriesProcessor(RaftServer server) {
        this(server, Collections.emptyList());
    }

    public AppendEntriesProcessor(RaftServer server, List<RaftCommandListener<AppendEntriesCommand>> listeners) {
        super(server, listeners);
    }

    @Override
    protected AppendEntriesCommand decodeRemotingCommand(RemotingCommand request) {
        return new AppendEntriesCommand(request.getBody());
    }

    @Override
    protected RemotingCommand doProcess(AppendEntriesCommand appendCmd) {
        logger.debug("receive append entries command, cmd={}, server={}", appendCmd, this.server);
        final int termInEntry = appendCmd.getTerm();
        final RaftServer server = this.getServer();
        final RaftLog raftLog = server.getRaftLog();

        int termInServer = this.getServer().getTerm();
        final AppendEntriesCommand response = new AppendEntriesCommand(termInServer, this.getServer().getLeaderId());
        response.setSuccess(false);

        raftLog.getEntry(appendCmd.getPrevLogIndex()).ifPresent(prevEntry -> {
            if (prevEntry.getTerm() == appendCmd.getPrevLogTerm() && termInEntry >= termInServer) {
                if (!appendCmd.getLeaderId().equals(this.server.getLeaderId())) {
                    server.tryBecomeFollower(termInEntry, appendCmd.getLeaderId());
                }

                Optional<LogEntry> entry = appendCmd.getEntry();
                if (entry.isPresent()) {
                    try {
                        raftLog.appendEntries(entry.get());
                        response.setSuccess(true);
                        raftLog.tryCommitTo(appendCmd.getLeaderCommit());
                    } catch (Exception ex) {
                        logger.error("some thing wrong with append entries");
                    }
                } else {
                    raftLog.tryCommitTo(appendCmd.getLeaderCommit());
                }
            }
        });

        logger.debug("respond append entries command, response={}, server={}", response, this.server);
        return RemotingCommand.createResponseCommand(response);
    }
}
