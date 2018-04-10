package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/1/12
 */
class RaftPeerNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeerNode.class.getName());

    private final String peerId;
    private final RaftServer server;
    private final RaftLog serverLog;

    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private int matchIndex;

    RaftPeerNode(String peerId, RaftServer server, RaftLog log, int nextIndex) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
        this.server = server;
        this.serverLog = log;
    }

    void sendAppend(int maxMsgSize) {
        final int startIndex = this.nextIndex;
        final List<LogEntry> entries = serverLog.getEntries(startIndex - 1, startIndex + maxMsgSize);

        // at least two entries, one is prev LogEntry, the other is the newly append LogEntry
        assert entries.size() > 1;

        final LogEntry prev = entries.get(0);

        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES)
                .setTerm(this.server.getTerm())
                .setLeaderId(this.server.getLeaderId())
                .setLeaderCommit(serverLog.getCommitIndex())
                .setPrevLogIndex(prev.getIndex())
                .setPrevLogTerm(prev.getTerm())
                .addAllEntries(entries.subList(1, entries.size()))
                .setTo(peerId);
        server.writeOutCommand(msg);
    }

    void sendPing() {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PING)
                .setTerm(this.server.getTerm())
                .setLeaderId(this.server.getLeaderId())
                .setLeaderCommit(Math.min(this.matchIndex, serverLog.getCommitIndex()))
                .setTo(peerId);
        server.writeOutCommand(msg);
    }

    synchronized void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    int getMatchIndex() {
        return matchIndex;
    }

    public String getPeerId() {
        return peerId;
    }

    @Override
    public String toString() {
        return "RaftPeerNode{" +
                "peerId='" + peerId + '\'' +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
