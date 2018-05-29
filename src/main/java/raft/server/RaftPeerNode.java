package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.LogsCompactedException;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;
import raft.server.proto.Snapshot;

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
    private final RaftLog raftLog;

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
        this.raftLog = log;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
    }

    void sendAppend(int term) {
        final int startIndex = getNextIndex();
        boolean compacted = false;
        Optional<Integer> prevTerm;
        RaftCommand.Builder msgBuilder = RaftCommand.newBuilder()
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(raftLog.getCommitIndex())
                .setTerm(term)
                .setTo(peerId);

        try {
            prevTerm = raftLog.getTerm(startIndex - 1);
        } catch (LogsCompactedException ex) {
            compacted = true;
            prevTerm = Optional.empty();
        }

        if (compacted) {
            Optional<Snapshot> snapshot = raft.getRecentSnapshot(startIndex - 1);
            if (snapshot.isPresent()) {
                Snapshot s = snapshot.get();
                logger.info("prepare to send snapshot with index: {} term: {} to {}", s.getIndex(), s.getTerm(), this);
                msgBuilder.setType(RaftCommand.CmdType.SNAPSHOT).setSnapshot(snapshot.get());
            } else {
                logger.warn("snapshot on {} is not ready, skip append to {}", raft.getSelfId(), this);
                return;
            }
        } else {
            if (!prevTerm.isPresent()) {
                String m = String.format("get term for index: %s from log: %s failed", startIndex - 1, raftLog);
                throw new IllegalStateException(m);
            }

            List<LogEntry> entries = raftLog.getEntries(startIndex, startIndex + this.maxEntriesPerAppend);
            msgBuilder.setType(RaftCommand.CmdType.APPEND_ENTRIES)
                    .setPrevLogIndex(startIndex - 1)
                    .setPrevLogTerm(prevTerm.get())
                    .addAllEntries(entries.subList(1, entries.size()));
        }

        raft.writeOutCommand(msgBuilder);
    }

    void sendPing(int term) {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PING)
                .setTerm(term)
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(Math.min(getMatchIndex(), raftLog.getCommitIndex()))
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
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized void decreaseIndexAndResendAppend(int term) {
        nextIndex--;
        if (nextIndex < 1) {
            logger.warn("nextIndex for {} decreased to 1", this.toString());
            nextIndex = 1;
        }
        assert nextIndex > matchIndex : "nextIndex: " + nextIndex + " is not greater than matchIndex: " + matchIndex;
        sendAppend(term);
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized int getMatchIndex() {
        return matchIndex;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
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
