package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
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
    private final RaftImpl raft;
    private final RaftLog serverLog;

    // index of the next log entry to send to that raft (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on raft (initialized to 0, increases monotonically)
    private int matchIndex;
    private int maxEntriesPerAppend;

    RaftPeerNode(String peerId, RaftImpl raft, RaftLog log, int nextIndex, int maxEntriesPerAppend) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
        this.raft = raft;
        this.serverLog = log;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
    }

    void sendAppend(int term) {
        final int startIndex = this.nextIndex;
        final List<LogEntry> entries = serverLog.getEntries(startIndex - 1, startIndex + this.maxEntriesPerAppend);

        // entries could contains only one LogEntry when leader just want to update follower's commit index
        assert entries.size() > 0;

        final LogEntry prev = entries.get(0);

        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES)
                .setTerm(term)
                .setLeaderId(this.raft.getLeaderId())
                .setLeaderCommit(serverLog.getCommitIndex())
                .setPrevLogIndex(prev.getIndex())
                .setPrevLogTerm(prev.getTerm())
                .addAllEntries(entries.subList(1, entries.size()))
                .setTo(peerId);
        raft.writeOutCommand(msg);
    }

    void sendPing(int term) {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PING)
                .setTerm(term)
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(Math.min(matchIndex, serverLog.getCommitIndex()))
                .setTo(peerId);
        raft.writeOutCommand(msg);
    }

    void sendTimeout(int term) {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.TIMEOUT_NOW)
                .setTerm(term)
                .setLeaderId(raft.getLeaderId())
                .setTo(peerId);
        raft.writeOutCommand(msg);
    }

    boolean updateIndexes(int matchIndex) {
        boolean updated = false;
        if (this.matchIndex < matchIndex) {
            this.matchIndex = matchIndex;
            updated = true;
        }

        if (this.nextIndex < matchIndex + 1) {
            this.nextIndex = matchIndex + 1;
        }

        return updated;
    }

    void decreaseIndexAndResendAppend(int term) {
        nextIndex--;
        if (nextIndex < 1) {
            logger.warn("nextIndex for {} decreased to 1", this.toString());
            nextIndex = 1;
        }
        assert nextIndex > matchIndex: "nextIndex: " + nextIndex + " is not greater than matchIndex: " + matchIndex;
        sendAppend(term);
    }

    void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    int getMatchIndex() {
        return matchIndex;
    }

    String getPeerId() {
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
