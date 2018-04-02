package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */

@SuppressWarnings("WeakerAccess")
public class RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    // TODO better use ThreadPoolExecutor to give every thread pool a name
    private final ScheduledExecutorService tickGenerator = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService tickTimeoutExecutors = Executors.newFixedThreadPool(8);
    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();

    private final String selfId;
    private final RaftLog raftLog;
    private final long tickIntervalMs;
    private final int maxMsgSize;
    private final long pingIntervalTicks;
    private final long suggestElectionTimeoutTicks;

    // TODO need persistent
    private String votedFor;
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private AtomicInteger term;
    private String leaderId;
    private RaftState state;
    private long matchIndex;
    private long electionTimeoutTicks;
    private StateMachine stateMachine;

    public RaftServer(Config c, StateMachine stateMachine) {
        this.term = new AtomicInteger(0);
        this.selfId = c.selfId;
        this.raftLog = new RaftLog();
        this.tickIntervalMs = c.tickIntervalMs;
        this.suggestElectionTimeoutTicks = c.suggestElectionTimeoutTicks;
        this.pingIntervalTicks = c.pingIntervalTicks;
        this.stateMachine = stateMachine;
        this.maxMsgSize = c.maxMsgSize;

        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1));
        }

        this.state = follower;

        this.electionTimeoutTicks = this.generateElectionTimeoutTicks();
        this.reset();
    }

    private void reset() {
        this.votedFor = null;
        this.tickCount.set(0);
    }

    private void transitState(RaftState nextState) {
        assert this.stateLock.isLocked();

        if (this.state != nextState) {
            this.state.finish();

            this.state = nextState;
            nextState.start();
        }
    }

    private boolean tryBecomeFollower(int term, String leaderId) {
        this.stateLock.lock();
        try {
            if (term > this.term.get()) {
                this.leaderId = leaderId;
                this.term.set(term);
                this.reset();
                this.transitState(follower);
                return true;
            } else {
                logger.warn("node {} transient state to {} failed, term = {}, leaderId = {}",
                        this, State.FOLLOWER, term, leaderId);
                return false;
            }
        } finally {
            this.stateLock.unlock();
        }
    }

    private boolean tryBecomeLeader() {
        this.stateLock.lock();
        try {
            if (this.getState() == State.CANDIDATE) {
                this.leaderId = this.selfId;
                this.reset();
                this.transitState(leader);

                // reinitialize nextIndex for every peer node
                // then send them initial empty AppendEntries RPC (heartbeat)
                for (RaftPeerNode node : this.peerNodes.values()) {
                    node.reset(this.raftLog.getLastIndex() + 1);
                    node.sendAppend(RaftServer.this.maxMsgSize);
                }
                return true;
            } else {
                logger.warn("node {} transient state to {} failed", this, State.LEADER);
                return false;
            }
        } finally {
            this.stateLock.unlock();
        }
    }

    void start() {
        this.state.start();

        this.tickGenerator.scheduleWithFixedDelay(() -> {
            long tick = this.tickCount.incrementAndGet();
            if (this.state.isTickTimeout(tick)) {
                this.tickCount.set(0);
                try {
                    this.tickTimeoutExecutors.submit(() -> {
                        this.stateLock.lock();
                        try {
                            this.state.onTickTimeout();
                        } catch (Throwable t) {
                            logger.error("process tick timeout failed on tick {} of state {}", tick, this.state.getState(), t);
                        } finally {
                            this.stateLock.unlock();
                        }
                    });
                } catch (RejectedExecutionException ex) {
                    logger.error("submit process tick timeout job failed", ex);
                }
            }

        }, this.tickIntervalMs, this.tickIntervalMs, TimeUnit.MILLISECONDS);
    }

    public int getTerm() {
        return this.term.get();
    }

    public void setVotedFor(String votedFor) {
        this.stateLock.lock();
        try {
            this.votedFor = votedFor;
        } finally {
            this.stateLock.unlock();
        }
    }

    public void clearTickCount() {
        this.tickCount.set(0);
    }

    private void setLeaderId(String leaderId) {
        if (!leaderId.equals(this.leaderId)) {
            this.stateLock.lock();
            try {
                this.leaderId = leaderId;
            } finally {
                this.stateLock.unlock();
            }
        }
    }

    public boolean isLeader() {
        return this.getState() == State.LEADER;
    }

    private long generateElectionTimeoutTicks() {
        return RaftServer.this.suggestElectionTimeoutTicks +
                ThreadLocalRandom.current().nextLong(RaftServer.this.suggestElectionTimeoutTicks);
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return this.peerNodes;
    }

    private int getQuorum() {
        return this.peerNodes.size() / 2 + 1;
    }

    private State getState() {
        return this.state.getState();
    }

    public String getLeaderId() {
        return this.leaderId;
    }

    public String getSelfId() {
        return selfId;
    }

    public RaftStatus getStatus() {
        RaftStatus status = new RaftStatus();
        status.setTerm(this.getTerm());
        status.setCommitIndex(this.raftLog.getCommitIndex());
        status.setAppliedIndex(this.raftLog.getAppliedIndex());
        status.setId(this.selfId);
        status.setLeaderId(this.leaderId);
        status.setVotedFor(this.votedFor);
        status.setState(this.getState());

        return status;
    }

    void appliedTo(int appliedTo) {
        this.raftLog.appliedTo(appliedTo);
    }

    ProposeResponse propose(List<byte[]> entries) {
        String leaderId = this.getLeaderId();
        ErrorMsg error = null;
        this.stateLock.tryLock();
        try {
            if (this.getState() == State.LEADER) {
                int term = this.getTerm();
                this.raftLog.directAppend(term, entries);
                this.broadcastAppendEntries();
            } else {
                error = ErrorMsg.NOT_LEADER_NODE;
            }
        } catch (Throwable t) {
            logger.error("propose failed on node {}", this, t);
            error = ErrorMsg.INTERNAL_ERROR;
        } finally {
            this.stateLock.unlock();
        }

        return new ProposeResponse(leaderId, error);
    }

    private boolean replicateLogsOnFollower(RaftCommand cmd) {
        int prevIndex = cmd.getPrevLogIndex();
        int prevTerm = cmd.getPrevLogTerm();
        int leaderCommitId = cmd.getLeaderCommit();
        String leaderId = cmd.getLeaderId();
        List<raft.server.proto.LogEntry> entries = cmd.getEntriesList();
        this.stateLock.tryLock();
        try {
            State state = this.getState();
            if (state == State.FOLLOWER) {
                try {
                    this.setLeaderId(leaderId);
                    return raftLog.tryAppendEntries(prevIndex, prevTerm, leaderCommitId, entries);
                } catch (Exception ex) {
                    logger.error("append entries failed on node {}, leaderCommitId={}, leaderId, entries",
                            this, leaderCommitId, leaderId, entries, ex);
                }
            } else {
                logger.error("append logs failed on node {} due to invalid state: {}", this, state);
            }
        } finally {
            this.stateLock.unlock();
        }

        return false;
    }

    private void broadcastAppendEntries() {
        if (peerNodes.isEmpty()) {
            List<LogEntry> appliedLogs = this.raftLog.tryCommitTo(this.raftLog.getLastIndex());
            stateMachine.onProposalApplied(appliedLogs);
        } else {
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(this.selfId)) {
                    peer.sendAppend(RaftServer.this.maxMsgSize);
                }
            }
        }
    }

    void updateCommit() {
        // kth biggest number
        int k = this.getQuorum() - 1;
        List<Integer> matchedIndexes = peerNodes.values().stream()
                .map(RaftPeerNode::getMatchIndex).sorted().collect(Collectors.toList());
        int kthMatchedIndexes = matchedIndexes.get(k);
        Optional<LogEntry> kthLog = this.raftLog.getEntry(kthMatchedIndexes);
        if (kthLog.isPresent()) {
            LogEntry e = kthLog.get();
            if (e.getTerm() == this.getTerm()) {
                this.raftLog.tryCommitTo(kthMatchedIndexes);
            }
        }
    }

    void processReceivedCommand(RaftCommand cmd) {
        if (cmd.getType() == RaftCommand.CmdType.REQUEST_VOTE) {
            logger.debug("node {} received request vote command, request={}", this, cmd);
            final String candidateId = cmd.getFrom();
            boolean isGranted = false;

            if ((this.votedFor == null || this.votedFor.equals(candidateId)) &&
                    cmd.getTerm() >= this.getTerm() &&
                    this.raftLog.isUpToDate(cmd.getLastLogTerm(), cmd.getLastLogIndex())) {
                this.setVotedFor(candidateId);
                this.clearTickCount();
                isGranted = true;
            }

            RaftCommand resp = RaftCommand.newBuilder()
                    .setType(RaftCommand.CmdType.REQUEST_VOTE_RESP)
                    .setVoteGranted(isGranted)
                    .setTo(cmd.getFrom())
                    .setFrom(this.selfId)
                    .build();
            this.stateMachine.onWriteCommand(resp);
        } else {
            // check term
            if (cmd.getTerm() > this.getTerm()) {
                this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
            }

            state.process(cmd);
        }
    }

    void shutdown() {
        this.tickGenerator.shutdown();
        this.tickTimeoutExecutors.shutdown();
    }

    @Override
    public String toString() {
        return "{" +
                "term=" + term +
                ", id='" + selfId + '\'' +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + this.getState() +
                '}';
    }

    class Leader extends RaftState {
        Leader() {
            super(State.LEADER);
        }

        public void start() {
            logger.debug("node {} start leader", RaftServer.this);
            RaftServer.this.broadcastAppendEntries();
        }

        public void finish() {
            logger.debug("node {} finish leader", RaftServer.this);
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftServer.this.pingIntervalTicks;
        }

        @Override
        public void onTickTimeout() {
            RaftServer.this.broadcastAppendEntries();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES_RESP:
                    if (cmd.getTerm() == RaftServer.this.getTerm()) {
                        RaftServer.this.replicateLogsOnFollower(cmd);
                    }
                default:
                    logger.warn("node {} received unexpected command {}", RaftServer.this, cmd);
            }
        }
    }

    class Follower extends RaftState {
        Follower() {
            super(State.FOLLOWER);
        }

        public void start() {
            logger.debug("node {} start follower", RaftServer.this);
        }

        public void finish() {
            logger.debug("node {} finish follower", RaftServer.this);
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftServer.this.electionTimeoutTicks;
        }

        private void becomeCandidate() {
            RaftServer.this.leaderId = null;
            RaftServer.this.reset();
            RaftServer.this.transitState(RaftServer.this.candidate);
        }

        @Override
        public void onTickTimeout() {
            assert RaftServer.this.stateLock.isLocked();

            logger.info("election timeout, node {} become candidate", RaftServer.this);

            this.becomeCandidate();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES:
                    RaftServer.this.clearTickCount();
                    RaftServer.this.replicateLogsOnFollower(cmd);
                default:
                    logger.warn("node {} received unexpected command {}", RaftServer.this, cmd);
            }
        }
    }

    class Candidate extends RaftState {
        private volatile ConcurrentHashMap<String, Boolean> votesGot = new ConcurrentHashMap<>();

        Candidate() {
            super(State.CANDIDATE);
        }

        public void start() {
            logger.debug("node {} start candidate", RaftServer.this);
            this.startElection();
        }

        public void finish() {
            logger.debug("node {} finish candidate", RaftServer.this);
        }

        private void startElection() {
            votesGot = new ConcurrentHashMap<>();
            RaftServer.this.votedFor = RaftServer.this.selfId;
            assert RaftServer.this.getState() == State.CANDIDATE;

            final int candidateTerm = RaftServer.this.term.incrementAndGet();

            // got self vote initially
            final int votesToWin = RaftServer.this.getQuorum() - 1;

            if (votesToWin == 0) {
                RaftServer.this.tryBecomeLeader();
            } else {
                Optional<LogEntry> lastEntry = RaftServer.this.raftLog.getEntry(RaftServer.this.raftLog.getLastIndex());
                assert lastEntry.isPresent();

                LogEntry e = lastEntry.get();

                logger.debug("node {} start election, votesToWin={}", RaftServer.this, votesToWin);
                for (final RaftPeerNode node : RaftServer.this.getPeerNodes().values()) {
                    if (!node.getPeerId().equals(RaftServer.this.selfId)) {
                        RaftCommand vote = RaftCommand.newBuilder()
                                .setType(RaftCommand.CmdType.REQUEST_VOTE)
                                .setTerm(candidateTerm)
                                .setFrom(RaftServer.this.selfId)
                                .setLastLogIndex(e.getIndex())
                                .setLastLogTerm(e.getTerm())
                                .setTo(node.getPeerId())
                                .build();
                        stateMachine.onWriteCommand(vote);
                    }
                }
            }
        }

        public void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case REQUEST_VOTE_RESP:
                    logger.debug("node {} received request vote response={}", RaftServer.this, cmd);

                    if (cmd.getTerm() == RaftServer.this.getTerm()) {
                        if (cmd.getVoteGranted()) {
                            final int votesToWin = RaftServer.this.getQuorum() - 1;
                            votesGot.put(cmd.getFrom(), true);
                            int votes = votesGot.values().stream().mapToInt(isGrunt -> isGrunt ? 1 : 0).sum();
                            if (votes >= votesToWin) {
                                RaftServer.this.tryBecomeLeader();
                            }
                        }
                    }
                case APPEND_ENTRIES:
                    // if term in cmd is greater than current term we are already transferred to follower
                    if (cmd.getTerm() == RaftServer.this.getTerm()) {
                        RaftServer.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                        RaftServer.this.replicateLogsOnFollower(cmd);
                    }
                default:
                    logger.warn("node {} received unexpected command {}", RaftServer.this, cmd);
            }
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftServer.this.electionTimeoutTicks;
        }

        @Override
        public void onTickTimeout() {
            this.startElection();
        }
    }
}
