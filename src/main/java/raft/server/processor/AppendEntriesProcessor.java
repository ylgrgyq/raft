package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractProcessor<AppendEntriesCommand>{
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
    protected RemotingCommand doProcess(AppendEntriesCommand entry) {
        logger.debug("receive append entries command, cmd={}, server={}", entry, this.server);
        final int termInEntry = entry.getTerm();
        final RaftServer server = this.getServer();
        if (! entry.getLeaderId().equals(this.server.getLeaderId())) {
            server.tryTransitStateToFollower(termInEntry, entry.getLeaderId());
        }

        AppendEntriesCommand response = new AppendEntriesCommand(this.getServer().getTerm());
        response.markSuccess();
        response.setLeaderId(this.getServer().getLeaderId());

        logger.debug("respond append entries command, response={}, server={}", response, this.server);
        return RemotingCommand.createResponseCommand(response);
    }
}
