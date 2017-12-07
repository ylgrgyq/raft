package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public class AppendEntriesProcessor extends AbstractProcessor{
    public AppendEntriesProcessor(RaftServer server) {
        super(server);
    }

    @Override
    public RemotingCommand processRequest(RemotingCommand request) {
        AppendEntriesCommand entry = new AppendEntriesCommand(request.getBody());

        System.out.println("Receive msg: " + entry);

        int term = entry.getTerm();
        final RaftServer server = this.getServer();
        server.checkTermThenTransferStateToFollower(term, entry.getLeaderId());

        AppendEntriesCommand response = new AppendEntriesCommand(this.getServer().getTerm());
        response.markSuccess();
        response.setLeaderId(this.getServer().getLeaderId());

        return RemotingCommand.createResponseCommand(response);
    }
}
