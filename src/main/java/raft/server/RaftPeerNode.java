package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.LogsCompactedException;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;
import raft.server.proto.LogSnapshot;

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
    private long nextIndex;
    // index of highest log entry known to be replicated on raft (initialized to 0, increases monotonically)
    private long matchIndex;
    private int maxEntriesPerAppend;

    RaftPeerNode(String peerId, RaftImpl raft, RaftLog log, long nextIndex, int maxEntriesPerAppend) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = -1L;
        this.raft = raft;
        this.raftLog = log;
        this.maxEntriesPerAppend = maxEntriesPerAppend;
    }

    void sendAppend(long term) {
        final long startIndex = getNextIndex();
        boolean compacted = false;
        Optional<Long> prevTerm;
        RaftCommand.Builder msgBuilder = RaftCommand.newBuilder()
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(raftLog.getCommitIndex())
                .setTerm(term)
                .setTo(peerId);

        logger.debug("node {} send append to {} with logs start from {}", raft, this, startIndex);

        try {
            prevTerm = raftLog.getTerm(startIndex - 1);
        } catch (LogsCompactedException ex) {
            compacted = true;
            prevTerm = Optional.empty();
        }

        if (compacted) {
            Optional<LogSnapshot> snapshot = raft.getRecentSnapshot(startIndex - 1);
            if (snapshot.isPresent()) {
                LogSnapshot s = snapshot.get();
                logger.info("prepare to send snapshot with index: {} term: {} to {}", s.getIndex(), s.getTerm(), this);
                msgBuilder.setType(RaftCommand.CmdType.SNAPSHOT).setSnapshot(snapshot.get());
            } else {
                logger.info("snapshot on {} is not ready, skip append to {}", raft.getId(), this);
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
                    .addAllEntries(entries);
        }

        raft.writeOutCommand(msgBuilder);
    }

    void sendPing(long term) {
        RaftCommand.Builder msg = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PING)
                .setTerm(term)
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(Math.min(getMatchIndex(), raftLog.getCommitIndex()))
                .setTo(peerId);
        raft.writeOutCommand(msg);
    }

    void sendTimeout(long term) {
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
    synchronized boolean updateIndexes(long matchIndex) {
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
    synchronized void decreaseIndexAndResendAppend(long term) {
        nextIndex--;
        if (nextIndex < 1L) {
            logger.warn("nextIndex for {} decreased to 1", this.toString());
            nextIndex = 1L;
        }
        assert nextIndex > matchIndex : "nextIndex: " + nextIndex + " is not greater than matchIndex: " + matchIndex;
        sendAppend(term);
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized void reset(long nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = -1L;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized long getMatchIndex() {
        return matchIndex;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    private synchronized long getNextIndex() {
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
