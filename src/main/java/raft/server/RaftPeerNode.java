package raft.server;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteClient;
import raft.server.rpc.*;

/**
 * Author: ylgrgyq
 * Date: 18/1/12
 */
public class RaftPeerNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeerNode.class.getName());

    private RemoteClient remoteClient;
    private RaftServer server;
    private RaftLog serverLog;

    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private int matchIndex;

    RaftPeerNode(RaftServer server, RaftLog log, RemoteClient remote, int nextIndex) {
        this.remoteClient = remote;
        this.nextIndex = nextIndex;
        this.matchIndex = 1;
        this.server = server;
        this.serverLog = log;
    }

    void sendAppend() {
        LogEntry entry = serverLog.getEntry(nextIndex);
        LogEntry prevEntry = serverLog.getEntry(nextIndex - 1);

        AppendEntriesCommand appendReq = new AppendEntriesCommand(this.server.getTerm());
        appendReq.setPrevLogTerm(prevEntry.getTerm());
        appendReq.setPrevLogIndex(prevEntry.getIndex());
        appendReq.setEntry(entry);
        appendReq.setLeaderCommit(serverLog.commitIndex);

        RemotingCommand cmd = RemotingCommand.createRequestCommand(appendReq);
        remoteClient.send(cmd, (PendingRequest req) -> {
            final RemotingCommand res = req.getResponse();
            if (res != null) {
                final AppendEntriesCommand appendRes = new AppendEntriesCommand(res.getBody());
                if (appendRes.isSuccess()){

                } else {

                }
            }
        });
    }

    Future<Void> send(RemotingCommand cmd, PendingRequestCallback callback) {
        return this.remoteClient.send(cmd, callback);
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
        if (matchIndex >= this.nextIndex) {
            this.nextIndex = matchIndex + 1;
        }
    }
}
