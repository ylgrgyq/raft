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
    private final PeerNodeInflights inflights;
    private final int maxEntriesPerAppend;

    // index of the next log entry to send to that raft (initialized to leader last log index + 1)
    private long nextIndex;
    // index of highest log entry known to be replicated on raft (initialized to 0, increases monotonically)
    private long matchIndex;
    private PeerNodeState state;
    private boolean pause;

    RaftPeerNode(String peerId, RaftImpl raft, RaftLog log, long nextIndex, int maxEntriesPerAppend) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = -1L;
        this.raft = raft;
        this.raftLog = log;
        this.inflights = new PeerNodeInflights(maxEntriesPerAppend);
        this.maxEntriesPerAppend = maxEntriesPerAppend;
        this.state = new ReplicateState();
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
            Optional<LogSnapshot> snapshot = raft.getRecentSnapshot(nextIndex - 1);
            if (snapshot.isPresent()) {
                LogSnapshot s = snapshot.get();
                logger.info("prepare to send snapshot with index: {} term: {} to {}", s.getIndex(), s.getTerm(), this);
                msgBuilder.setType(RaftCommand.CmdType.SNAPSHOT)
                        .setSnapshot(snapshot.get());
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

        if (state == replicateState) {
            transferToProbe();
        }
        sendAppend(term, false);
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
                ", state=" + state.stateName() +
                ", inflights=" + inflights.size() +
                '}';
    }

    private void transferToReplicate() {
        state = replicateState;
    }

    private void transferToProbe() {
        state= probeState;
    }

    private void transferToSnapshot() {
        state= snapshotState;
    }

    interface PeerNodeState {
        String stateName();
        void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend);
        void onReceiveAppendSuccess(RaftPeerNode node, long successIndex);
        boolean nodeIsPaused(RaftPeerNode node);
    }

    static class ProbeState implements  PeerNodeState {
        @Override
        public String stateName() {
            return "Probe";
        }

        @Override
        public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
            node.pause();
        }

        @Override
        public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
            node.transferToReplicate();
        }

        @Override
        public boolean nodeIsPaused(RaftPeerNode node) {
            return node.pause;
        }
    }

    static class ReplicateState implements PeerNodeState {
        @Override
        public String stateName() {
            return "Replicate";
        }

        @Override
        public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
            long lastEntryIndex = entriesToSend.get(entriesToSend.size() - 1).getIndex();
            node.inflights.addInflightIndex(lastEntryIndex);
            node.nextIndex = lastEntryIndex + 1;
        }

        @Override
        public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
            node.inflights.freeTo(successIndex);
        }

        @Override
        public boolean nodeIsPaused(RaftPeerNode node) {
            return node.inflights.isFull();
        }
    }

    static class SnapshotState implements PeerNodeState {
        @Override
        public String stateName() {
            return "Snapshot";
        }

        @Override
        public void onSendAppend(RaftPeerNode node, List<LogEntry> entriesToSend) {
            assert false;
            logger.error("send append msg to peer: {} on snapshot state", node);
        }

        @Override
        public void onReceiveAppendSuccess(RaftPeerNode node, long successIndex) {
            node.transferToProbe();
        }

        @Override
        public boolean nodeIsPaused(RaftPeerNode node) {
            return true;
        }
    }
}
