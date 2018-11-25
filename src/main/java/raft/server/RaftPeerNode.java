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
    private static final ReplicateState replicateState = new ReplicateState();
    private static final ProbeState probeState = new ProbeState();
    private static final SnapshotState snapshotState = new SnapshotState();

    private final String peerId;
    private final RaftImpl raft;
    private final RaftLog raftLog;
    private final int maxEntriesPerAppend;

    // index of the next log entry to send to that raft (initialized to leader last log index + 1)
    private long nextIndex;
    // index of highest log entry known to be replicated on raft (initialized to 0, increases monotonically)
    private long matchIndex;
    private PeerNodeState state;

    final PeerNodeInflights inflights;

    long pendingSnapshotIndex;
    boolean pause;

    RaftPeerNode(String peerId, RaftImpl raft, RaftLog log, long nextIndex, int maxEntriesPerAppend) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = 0L;
        this.raft = raft;
        this.raftLog = log;
        this.inflights = new PeerNodeInflights(maxEntriesPerAppend);
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        this.state = replicateState;
    }

    boolean sendAppend(long term, boolean allowEmpty) {
        if (isPaused()) {
            return false;
        }

        final long nextIndex = getNextIndex();
        boolean compacted = false;
        Optional<Long> prevTerm;
        RaftCommand.Builder msgBuilder = RaftCommand.newBuilder()
                .setLeaderId(raft.getLeaderId())
                .setLeaderCommit(raftLog.getCommitIndex())
                .setTerm(term)
                .setTo(peerId);

        logger.debug("node {} send append to {} with logs start from {}", raft, this, nextIndex);

        try {
            prevTerm = raftLog.getTerm(nextIndex - 1);
        } catch (LogsCompactedException ex) {
            compacted = true;
            prevTerm = Optional.empty();
        }

        if (compacted) {
            Optional<LogSnapshot> snapshotOpt = raft.getRecentSnapshot(nextIndex - 1);
            if (snapshotOpt.isPresent()) {
                LogSnapshot snapshot = snapshotOpt.get();
                logger.info("prepare to send snapshot with index: {} term: {} to {}", snapshot.getIndex(), snapshot.getTerm(), this);
                msgBuilder.setType(RaftCommand.CmdType.SNAPSHOT)
                        .setSnapshot(snapshot);
                pendingSnapshotIndex = snapshot.getIndex();
                transferToSnapshot();
            } else {
                logger.info("snapshot on {} is not ready, skip append to {}", raft.getId(), this);
                return false;
            }
        } else {
            if (!prevTerm.isPresent()) {
                String m = String.format("get term for index: %s from log: %s failed", nextIndex - 1, raftLog);
                throw new IllegalStateException(m);
            }

            msgBuilder.setType(RaftCommand.CmdType.APPEND_ENTRIES)
                    .setPrevLogIndex(nextIndex - 1)
                    .setPrevLogTerm(prevTerm.get());

            List<LogEntry> entries = raftLog.getEntries(nextIndex, nextIndex + maxEntriesPerAppend);
            if (entries.isEmpty()) {
                if (!allowEmpty) {
                    return false;
                }
            } else {
                state.onSendAppend(this, entries);
                msgBuilder.addAllEntries(entries);
            }
        }

        raft.writeOutCommand(msgBuilder);
        return true;
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

    boolean isPaused() {
        return state.nodeIsPaused(this);
    }

    void pause() {
        this.pause = true;
    }

    void resume() {
        this.pause = false;
    }



    void onReceiveAppendSuccess(long successIndex) {
        state.onReceiveAppendSuccess(this, successIndex);
    }

    void onPongRecieved() {
        // only allow sending a single probing append to follower during a ping interval to
        // reduce the cost in probing state. If we probe on every APPEND_ENTRIES_RESP, we may send
        // a lot of probing append in a short time
        resume();
        state.onPongReceived(this);
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
        if (state == replicateState) {
            transferToProbe();
        } else {
            nextIndex--;
            if (nextIndex < 1L) {
                logger.warn("nextIndex for {} decreased to 1", this.toString());
                nextIndex = 1L;
            }
            assert nextIndex > matchIndex : "nextIndex: " + nextIndex + " is not greater than matchIndex: " + matchIndex;
        }

        sendAppend(term, false);
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized void reset(long nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0L;
        this.inflights.reset();
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    synchronized long getMatchIndex() {
        return matchIndex;
    }

    void setMatchIndex(long index) {
        this.matchIndex = index;
    }

    /**
     * synchronized mark is used to protect matchIndex and nextIndex which may be contended between
     * raft main worker thread and leader async append log thread
     */
    private synchronized long getNextIndex() {
        return nextIndex;
    }

    void setNextIndex(long nextIndex) {
        this.nextIndex = nextIndex;
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
                ", state=" + state.stateName() +
                ", inflights=" + inflights.size() +
                '}';
    }

    void transferToReplicate(long matchIndex) {
        state = replicateState;
        pause = false;
        inflights.reset();
        nextIndex = matchIndex + 1;
    }

    void transferToProbe() {
        state= probeState;
        pause = false;
        inflights.reset();
        nextIndex = matchIndex + 1;
    }

    void transferToSnapshot() {
        pause = false;
        inflights.reset();
        state= snapshotState;
    }
}
