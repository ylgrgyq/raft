package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.Optional;

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
        final int startIndex = getNextIndex();
        final Optional<Integer> prevTerm = serverLog.getTerm(startIndex - 1);
        final List<LogEntry> entries = serverLog.getEntries(startIndex, startIndex + this.maxEntriesPerAppend);

        //TODO currently we suppose we can get prevTerm, but we need to fix this when we have snapshot
        assert prevTerm.isPresent();

        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES)
                .setTerm(term)
                .setLeaderId(this.raft.getLeaderId())
                .setLeaderCommit(serverLog.getCommitIndex())
                .setPrevLogIndex(startIndex - 1)
                .setPrevLogTerm(prevTerm.get())
                .addAllEntries(entries.subList(1, entries.size()))
                .setTo(peerId);
        raft.writeOutCommand(msg);
    }

    void sendPing(int term) {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PING)
                .setTerm(term)
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(Math.min(getMatchIndex(), serverLog.getCommitIndex()))
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

    // raft main worker thread and leader async append log thread may contend lock to update matchIndex and nextIndex

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended by
     * raft main worker thread and leader async append log thread
     *
     */
    synchronized boolean updateIndexes(int matchIndex) {
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

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended by
     * raft main worker thread and leader async append log thread
     *
     */
    synchronized void decreaseIndexAndResendAppend(int term) {
        nextIndex--;
        if (nextIndex < 1) {
            logger.warn("nextIndex for {} decreased to 1", this.toString());
            nextIndex = 1;
        }
        assert nextIndex > matchIndex: "nextIndex: " + nextIndex + " is not greater than matchIndex: " + matchIndex;
        sendAppend(term);
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended by
     * raft main worker thread and leader async append log thread
     *
     */
    synchronized void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended by
     * raft main worker thread and leader async append log thread
     *
     */
    synchronized int getMatchIndex() {
        return matchIndex;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended by
     * raft main worker thread and leader async append log thread
     *
     */
    private synchronized int getNextIndex() {
        return nextIndex;
    }

    String getPeerId() {
        return peerId;
    }

    @Override
    public synchronized String toString() {
        return "RaftPeerNode{" +
                "peerId='" + peerId + '\'' +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
