package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftServer;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractProcessor{
    private static final Logger logger = LoggerFactory.getLogger(AppendEntriesProcessor.class.getName());

    public AppendEntriesProcessor(RaftServer server) {
        super(server);
    }

    @Override
    public RemotingCommand processRequest(RemotingCommand request) {
        final AppendEntriesCommand entry = new AppendEntriesCommand(request.getBody());

        logger.info("Receive msg: " + entry);

        final int termInEntry = entry.getTerm();
        final RaftServer server = this.getServer();
        server.checkTermThenTransferStateToFollower(termInEntry, entry.getLeaderId());

        AppendEntriesCommand response = new AppendEntriesCommand(this.getServer().getTerm());
        response.markSuccess();
        response.setLeaderId(this.getServer().getLeaderId());

        return RemotingCommand.createResponseCommand(response);
    }
}
