package raft.server;

import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.PendingRequest;
import raft.server.rpc.PendingRequestCallback;
import raft.server.rpc.RemotingCommand;

import java.util.List;

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
        this.matchIndex = 0;
        this.server = server;
        this.serverLog = log;
    }

    void sendAppend() {
        List<LogEntry> entries = serverLog.getEntries(this.nextIndex - 1, this.nextIndex + 1);

        AppendEntriesCommand appendReq = new AppendEntriesCommand(this.server.getTerm(), this.server.getLeaderId());
        appendReq.setLeaderCommit(serverLog.getCommitIndex());

        LogEntry prev = entries.get(0);
        appendReq.setPrevLogTerm(prev.getTerm());
        appendReq.setPrevLogIndex(prev.getIndex());

        if (entries.size() > 1) {
            appendReq.setEntry(entries.get(1));
        }

        RemotingCommand cmd = RemotingCommand.createRequestCommand(appendReq);
        remoteClient.send(cmd, (PendingRequest req, RemotingCommand res) -> {
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

    Future<Void> sendOneway(RemotingCommand cmd) {
        return this.remoteClient.sendOneway(cmd);
    }

    void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }
}
