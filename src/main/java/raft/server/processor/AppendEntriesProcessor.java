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
        AppendEntriesCommand entry = new AppendEntriesCommand();
        entry.decode(request.getBody());
        int term = request.getTerm();
        final RaftServer server = this.getServer();
        if (term >= server.getTerm()) {
            server.setStateToFollower(entry.getLeaderId());
        }
        System.out.println("Receive msg: " + entry);
        return null;
    }
}
