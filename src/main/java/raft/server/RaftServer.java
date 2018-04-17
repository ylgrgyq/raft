package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicBoolean tickerTimeout = new AtomicBoolean();
    private final AtomicBoolean wakenUp = new AtomicBoolean();
    private BlockingQueue<Proposal> proposalQueue = new LinkedBlockingQueue<>(1000);
    private BlockingQueue<RaftCommand> receivedCmdQueue = new LinkedBlockingQueue<>(1000);

    private final ScheduledExecutorService tickGenerator;
    private final ExecutorService stateMachineJobExecutors;
    private final String selfId;
    private final RaftLog raftLog;
    private final long tickIntervalMs;
    private final int maxEntriesPerAppend;
    private final long pingIntervalTicks;

    private final long suggestElectionTimeoutTicks;
    // TODO need persistent
    private String votedFor;
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private AtomicInteger term;
    private String leaderId;
    private RaftState state;
    private long electionTimeoutTicks;
    private StateMachine stateMachine;
    private Thread workerThread;
    private volatile boolean workerRun = true;

    RaftServer(Config c, StateMachine stateMachine) {
        this.workerThread = new Thread(this);
        this.term = new AtomicInteger(0);
        this.raftLog = new RaftLog();
        this.stateMachineJobExecutors = Executors.newFixedThreadPool(8, new ThreadFactoryImpl("state-machine-executors-"));
        this.tickGenerator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("tick-generator-"));

        this.selfId = c.selfId;
        this.tickIntervalMs = c.tickIntervalMs;
        this.suggestElectionTimeoutTicks = c.suggestElectionTimeoutTicks;
        this.pingIntervalTicks = c.pingIntervalTicks;
        this.stateMachine = stateMachine;
        this.maxEntriesPerAppend = c.maxEntriesPerAppend;

        //TODO consider do not include selfId into peerNodes ?
        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1, RaftServer.this.maxEntriesPerAppend));
        }

        this.state = follower;

        this.reset(0);
    }

    void start() {
        this.tickGenerator.scheduleWithFixedDelay(() -> {
            long tick = this.tickCount.incrementAndGet();
            if (this.state.isTickTimeout(tick)) {
                clearTickCount();
                markTickerTimeout();
                wakeUpWorker();
            }

        }, this.tickIntervalMs, this.tickIntervalMs, TimeUnit.MILLISECONDS);

        this.workerThread.start();

        logger.info("node {} started with:\n" +
                        "electionTimeout={}\n" +
                        "tickIntervalMs={}\n" +
                        "pingIntervalTicks={}\n" +
                        "suggectElectionTimeoutTicks={}\n" +
                        "raftLog={}\n",
                this, this.electionTimeoutTicks, this.tickIntervalMs, this.pingIntervalTicks,
                this.suggestElectionTimeoutTicks, this.raftLog);
    }

    private void markTickerTimeout() {
        tickerTimeout.compareAndSet(false, true);
    }

    int getTerm() {
        return this.term.get();
    }

    private void clearTickCount() {
        this.tickCount.set(0);
    }

    boolean isLeader() {
        return this.getState() == State.LEADER;
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return this.peerNodes;
    }

    private int getQuorum() {
        return this.peerNodes.size() / 2 + 1;
    }

    State getState() {
        return this.state.getState();
    }

    String getLeaderId() {
        return this.leaderId;
    }

    String getSelfId() {
        return selfId;
    }

    // FIXME state may change during getting status
    RaftStatus getStatus() {
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

    CompletableFuture<ProposeResponse> propose(List<byte[]> entries) {
        String leaderId = this.getLeaderId();
        CompletableFuture<ProposeResponse> future;
        if (this.getState() == State.LEADER) {
            future = new CompletableFuture<>();
            this.proposalQueue.add(new Proposal(entries, future));
            wakeUpWorker();
        } else {
            future = CompletableFuture.completedFuture(new ProposeResponse(leaderId, ErrorMsg.NOT_LEADER_NODE));
        }
        return future;
    }

    private void wakeUpWorker() {
        if (wakenUp.compareAndSet(false, true)) {
            workerThread.interrupt();
        }
    }

    void writeOutCommand(RaftCommand.Builder builder) {
        builder.setFrom(this.selfId);
        try {
            stateMachineJobExecutors.submit(() -> this.stateMachine.onWriteCommand(builder.build()));
        } catch (RejectedExecutionException ex) {
            logger.error("node {} submit write out command job failed", ex);
        }
    }

    void queueReceivedCommand(RaftCommand cmd) {
        if (!this.peerNodes.containsKey(cmd.getFrom())) {
            logger.warn("node {} received cmd {} from unknown peer", this, cmd);
            return;
        }

        this.receivedCmdQueue.add(cmd);
    }

    void processTickTimeout() {
        if (tickerTimeout.compareAndSet(true, false)) {
            try {
                this.state.onTickTimeout();
            } catch (Throwable t) {
                logger.error("process tick timeout failed on node {}", this, t);
            }
        }
    }

    @Override
    public void run() {
        // init state
        RaftServer.this.state.start();

        while (workerRun) {
            try {
                processTickTimeout();

                List<RaftCommand> cmds = pollReceivedCmd();
                long start = System.nanoTime();
                processCommands(cmds);
                long now = System.nanoTime();
                long processCmdTime = now - start;

                long deadline = now + processCmdTime;
                long processedProposals = 0;
                Proposal p;
                while ((p = RaftServer.this.proposalQueue.poll()) != null) {
                    processProposal(p);
                    processedProposals++;
                    // check weather we have passed the deadline every 64 proposals
                    // so we can reduce the calling times of System.nanoTime and
                    // avoid the impact of narrow deadline
                    if ((processedProposals & 0x3F) == 0) {
                        if (System.nanoTime() >= deadline) {
                            break;
                        }
                    }
                }
            } catch (Throwable t) {
                logger.error("node {} got unexpected exception", this, t);
            }
        }
    }

    private List<RaftCommand> pollReceivedCmd() {
        // TODO maybe we can reuse this List on every run
        List<RaftCommand> cmds = Collections.emptyList();
        boolean initialed = false;
        RaftCommand cmd;
        try {
            wakenUp.getAndSet(false);
            // if wakenUp is set to true here between set wakenUp to false and poll the queue,
            // the poll will throw InterruptedException
            cmd = RaftServer.this.receivedCmdQueue.poll(1, TimeUnit.SECONDS);
            if (cmd != null) {
                cmds = new ArrayList<>();
                cmds.add(cmd);
                initialed = true;
            }
        } catch (InterruptedException ex) {
            processTickTimeout();
        }

        int i = 0;
        while (i < 1000 && (cmd = RaftServer.this.receivedCmdQueue.poll()) != null) {
            if (!initialed) {
                cmds = new ArrayList<>();
            }

            cmds.add(cmd);
            ++i;
        }

        return cmds;
    }

    private void processCommands(List<RaftCommand> cmds) {
        for (RaftCommand cmd : cmds) {
            try {
                processReceivedCommand(cmd);
            } catch (Throwable t) {
                logger.error("process received command {} on node {} failed", cmd, this, t);
            }
        }
    }

    private void processReceivedCommand(RaftCommand cmd) {
        if (cmd.getType() == RaftCommand.CmdType.REQUEST_VOTE) {
            logger.debug("node {} received request vote command, request={}", this, cmd);
            final String candidateId = cmd.getFrom();
            boolean isGranted = false;

            // each server will vote for at most one candidate in a given term, on a first-come-first-served basis
            // so we only need to check votedFor when term in this command equals to the term of this raft server
            if ((cmd.getTerm() > this.getTerm() || (cmd.getTerm() == this.getTerm() && (this.votedFor == null || this.votedFor.equals(candidateId))))
                    && this.raftLog.isUpToDate(cmd.getLastLogTerm(), cmd.getLastLogIndex())) {
                isGranted = true;
                this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                this.votedFor = candidateId;
            }

            RaftCommand.Builder resp = RaftCommand.newBuilder()
                    .setType(RaftCommand.CmdType.REQUEST_VOTE_RESP)
                    .setVoteGranted(isGranted)
                    .setTo(cmd.getFrom())
                    .setTerm(cmd.getTerm());
            writeOutCommand(resp);
        } else {
            // check term
            if (cmd.getTerm() < this.getTerm()) {
                RaftCommand.CmdType respType = null;
                RaftCommand.Builder resp = RaftCommand.newBuilder()
                        .setTo(cmd.getFrom())
                        .setSuccess(false)
                        .setTerm(getTerm());

                if (cmd.getType() == RaftCommand.CmdType.APPEND_ENTRIES) {
                    respType = RaftCommand.CmdType.APPEND_ENTRIES_RESP;
                } else if (cmd.getType() == RaftCommand.CmdType.PING) {
                    respType = RaftCommand.CmdType.PONG;
                }

                if (respType != null) {
                    resp.setType(respType);
                    writeOutCommand(resp);
                } else {
                    logger.warn("node {} receive unexpected command {} with term lower than current term", this, cmd);
                }
                return;
            }

            if (cmd.getTerm() > this.getTerm()) {
                this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
            }

            state.process(cmd);
        }
    }

    private void processProposal(Proposal proposal) {
        ErrorMsg error = null;
        try {
            if (this.getState() == State.LEADER) {
                int term = this.getTerm();
                this.raftLog.directAppend(term, proposal.entries);
                this.broadcastAppendEntries();
            } else {
                error = ErrorMsg.NOT_LEADER_NODE;
            }
        } catch (Throwable t) {
            logger.error("propose failed on node {}", this, t);
            error = ErrorMsg.INTERNAL_ERROR;
        }

        proposal.future.complete(new ProposeResponse(this.leaderId, error));
    }

    private void broadcastAppendEntries() {
        if (peerNodes.size() == 1) {
            this.raftLog.tryCommitTo(this.raftLog.getLastIndex());
            List<LogEntry> committedLogs = this.raftLog.getEntriesNeedToApply();
            writeOutCommitedLogs(committedLogs);
        } else {
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(this.selfId)) {
                    peer.sendAppend();
                }
            }
        }
    }

    private void writeOutCommitedLogs(List<LogEntry> commitedLogs) {
        try {
            stateMachineJobExecutors.submit(() -> this.stateMachine.onProposalCommited(commitedLogs));
        } catch (RejectedExecutionException ex) {
            logger.error("node {} submit apply proposal job failed", ex);
        }
    }

    private void broadcastPing() {
        for (final RaftPeerNode peer : peerNodes.values()) {
            if (!peer.getPeerId().equals(this.selfId)) {
                peer.sendPing();
            }
        }
    }

    private boolean tryBecomeFollower(int term, String leaderId) {
        assert Thread.currentThread() == workerThread;

        if (term >= this.term.get()) {
            this.reset(term);
            this.leaderId = leaderId;
            this.transitState(follower);
            return true;
        } else {
            logger.error("node {} transient state to {} failed, term = {}, leaderId = {}",
                    this, State.FOLLOWER, term, leaderId);
            return false;
        }
    }

    private boolean tryBecomeLeader() {
        assert Thread.currentThread() == workerThread;

        if (this.getState() == State.CANDIDATE) {
            this.reset(this.getTerm());
            this.leaderId = this.selfId;
            this.transitState(leader);

            // reinitialize nextIndex for every peer node
            // then send them initial ping on start leader state
            for (RaftPeerNode node : this.peerNodes.values()) {
                node.reset(this.raftLog.getLastIndex() + 1);
            }
            return true;
        } else {
            logger.error("node {} transient state to {} failed", this, State.LEADER);
            return false;
        }
    }

    private boolean tryBecomeCandidate() {
        assert Thread.currentThread() == workerThread;

        RaftServer.this.reset(this.getTerm());
        RaftServer.this.transitState(RaftServer.this.candidate);

        return true;
    }

    private void reset(int term) {
        // reset votedFor only when term changed
        // so when a candidate transit to leader it can keep votedFor to itself then when it receives
        // a request vote with the same term, it can reject that request
        if (this.term.getAndSet(term) != term) {
            this.votedFor = null;
        }
        this.leaderId = null;
        this.clearTickCount();
        this.tickerTimeout.getAndSet(false);

        // we need to reset election timeout on every time state changed and every
        // reelection in candidate state to avoid split vote
        this.electionTimeoutTicks = RaftServer.generateElectionTimeoutTicks(RaftServer.this.suggestElectionTimeoutTicks);
    }

    private static long generateElectionTimeoutTicks(long suggestElectionTimeoutTicks) {
        return suggestElectionTimeoutTicks + ThreadLocalRandom.current().nextLong(suggestElectionTimeoutTicks);
    }

    private void transitState(RaftState nextState) {
        assert Thread.currentThread() == workerThread;

        this.state.finish();
        this.state = nextState;
        nextState.start();
    }

    private void processAppendEntries(RaftCommand cmd) {
        RaftCommand.Builder resp = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES_RESP)
                .setTo(cmd.getFrom())
                .setTerm(getTerm())
                .setSuccess(false);
        if (cmd.getPrevLogIndex() < raftLog.getCommitIndex()) {
            resp.setMatchIndex(raftLog.getCommitIndex());
        } else {
            int matchIndex = RaftServer.this.replicateLogsOnFollower(cmd);
            if (matchIndex != 0) {
                resp.setSuccess(true);
                resp.setMatchIndex(matchIndex);
                writeOutCommitedLogs(raftLog.getEntriesNeedToApply());
            }
        }
        writeOutCommand(resp);
    }

    private int replicateLogsOnFollower(RaftCommand cmd) {
        int prevIndex = cmd.getPrevLogIndex();
        int prevTerm = cmd.getPrevLogTerm();
        int leaderCommitId = cmd.getLeaderCommit();
        String leaderId = cmd.getLeaderId();
        List<raft.server.proto.LogEntry> entries = cmd.getEntriesList();

        State state = this.getState();
        if (state == State.FOLLOWER) {
            try {
                this.leaderId = leaderId;
                return raftLog.tryAppendEntries(prevIndex, prevTerm, leaderCommitId, entries);
            } catch (Exception ex) {
                logger.error("append entries failed on node {}, leaderCommitId={}, leaderId, entries",
                        this, leaderCommitId, leaderId, entries, ex);
            }
        } else {
            logger.error("append logs failed on node {} due to invalid state: {}", this, state);
        }

        return 0;
    }

    private void processHeartbeat(RaftCommand cmd) {
        RaftCommand.Builder pong = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PONG)
                .setTo(cmd.getFrom())
                .setSuccess(true)
                .setTerm(getTerm());
        raftLog.tryCommitTo(cmd.getLeaderCommit());
        pong.setSuccess(true);
        writeOutCommand(pong);
        writeOutCommitedLogs(raftLog.getEntriesNeedToApply());
    }

    void shutdown() {
        this.tickGenerator.shutdown();
        this.stateMachineJobExecutors.shutdown();
        this.workerRun = false;
        wakeUpWorker();
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

    private static class Proposal {
        private final List<byte[]> entries;
        private final CompletableFuture<ProposeResponse> future;

        Proposal(List<byte[]> entries, CompletableFuture<ProposeResponse> future) {
            this.entries = new ArrayList<>(entries);
            this.future = future;
        }
    }

    private class Leader extends RaftState {
        Leader() {
            super(State.LEADER);
        }

        public void start() {
            logger.debug("node {} start leader", RaftServer.this);
            RaftServer.this.broadcastPing();
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
            RaftServer.this.broadcastPing();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES_RESP:
                    if (cmd.getTerm() > RaftServer.this.getTerm()) {
                        assert !cmd.getSuccess();
                        tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    } else if (cmd.getTerm() == RaftServer.this.getTerm()) {
                        RaftPeerNode node = peerNodes.get(cmd.getFrom());

                        assert node != null;

                        if (cmd.getSuccess()) {
                            if (node.updateIndexes(cmd.getMatchIndex()) && updateCommit()) {
                                broadcastAppendEntries();
                            }
                        } else {
                            node.decreaseIndexAndResendAppend();
                        }
                    }
                    break;
                case PONG:
                    // resend pending append entries
                    RaftPeerNode node = peerNodes.get(cmd.getFrom());
                    if (node.getMatchIndex() < RaftServer.this.raftLog.getLastIndex()) {
                        node.sendAppend();
                    }
                    break;
                case REQUEST_VOTE_RESP:
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftServer.this, cmd);
            }
        }

        private boolean updateCommit() {
            // kth biggest number
            int k = RaftServer.this.getQuorum() - 1;
            List<Integer> matchedIndexes = peerNodes.values().stream()
                    .map(RaftPeerNode::getMatchIndex).sorted().collect(Collectors.toList());
            int kthMatchedIndexes = matchedIndexes.get(k);
            Optional<LogEntry> kthLog = RaftServer.this.raftLog.getEntry(kthMatchedIndexes);
            if (kthLog.isPresent()) {
                LogEntry e = kthLog.get();
                // this is a key point. Raft never commits log entries from previous terms by counting replicas
                // Only log entries from the leader’s current term are committed by counting replicas; once an entry
                // from the current term has been committed in this way, then all prior entries are committed
                // indirectly because of the Log Matching Property
                if (e.getTerm() == RaftServer.this.getTerm()) {
                    RaftServer.this.raftLog.tryCommitTo(kthMatchedIndexes);
                    writeOutCommitedLogs(RaftServer.this.raftLog.getEntriesNeedToApply());
                    return true;
                }
            }
            return false;
        }
    }

    private class Follower extends RaftState {
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

        @Override
        public void onTickTimeout() {
            logger.info("election timeout, node {} become candidate", RaftServer.this);

            RaftServer.this.tryBecomeCandidate();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES:
                    RaftServer.this.clearTickCount();
                    RaftServer.this.leaderId = cmd.getLeaderId();
                    RaftServer.this.processAppendEntries(cmd);
                    break;
                case PING:
                    RaftServer.this.clearTickCount();
                    RaftServer.this.leaderId = cmd.getFrom();
                    RaftServer.this.processHeartbeat(cmd);
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftServer.this, cmd);
            }
        }
    }

    private class Candidate extends RaftState {
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
                    break;
                case APPEND_ENTRIES:
                    // if term in cmd is greater than current term we are already transferred to follower
                    RaftServer.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    RaftServer.this.processAppendEntries(cmd);
                    break;
                case PING:
                    RaftServer.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    RaftServer.this.processHeartbeat(cmd);
                    break;
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
            RaftServer.this.tryBecomeCandidate();
        }
    }
}
