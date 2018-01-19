package raft.server;

import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteClient;
import raft.server.rpc.*;

import java.util.ArrayList;
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
        List<LogEntry> entries = serverLog.getEntries(this.nextIndex - 1, this.nextIndex + this.server.getMaxMsgSize());

        AppendEntriesCommand appendReq = new AppendEntriesCommand(this.server.getTerm(), this.server.getLeaderId());
        appendReq.setLeaderCommit(serverLog.getCommitIndex());

        LogEntry prev = entries.get(0);
        appendReq.setPrevLogTerm(prev.getTerm());
        appendReq.setPrevLogIndex(prev.getIndex());
        appendReq.setEntries(entries.subList(1, entries.size()));

        RemotingCommand cmd = RemotingCommand.createRequestCommand(appendReq);
        this.send(cmd, (PendingRequest req, RemotingCommand res) -> {
            if (res.getBody().isPresent()) {
                final AppendEntriesCommand appendRes = new AppendEntriesCommand(res.getBody().get());
                if (appendRes.getTerm() > this.server.getTerm()){
                    this.server.tryBecomeFollower(appendRes.getTerm(), appendRes.getFrom());
                } else {
                    if (appendRes.isSuccess()) {
                        this.matchIndex = entries.get(entries.size() - 1).getIndex();
                        this.nextIndex = this.matchIndex + 1;
                    } else {
                        this.nextIndex--;
                        this.sendAppend();
                    }
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
