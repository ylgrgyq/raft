package raft.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.proto.ConfigChange;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftImpl implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RaftImpl.class.getName());

    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicBoolean tickerTimeout = new AtomicBoolean();
    private final AtomicBoolean wakenUp = new AtomicBoolean();
    private final BlockingQueue<Proposal> proposalQueue = new LinkedBlockingQueue<>(1000);
    private final BlockingQueue<RaftCommand> receivedCmdQueue = new LinkedBlockingQueue<>(1000);

    private final ScheduledExecutorService tickGenerator;
    private final ExecutorService stateMachineJobExecutors;
    private final String selfId;
    private final RaftLog raftLog;
    private final long tickIntervalMs;
    private final int maxEntriesPerAppend;
    private final long pingIntervalTicks;
    private final long suggestElectionTimeoutTicks;
    private final RaftPersistentState meta;
    private final AsyncNotifyStateMachineProxy stateMachine;
    private final RaftCommandBroker broker;

    private String leaderId;
    private RaftState state;
    private long electionTimeoutTicks;
    private Thread workerThread;
    private volatile boolean workerRun = true;
    private boolean existsPendingConfigChange = false;

    RaftImpl(Config c) {
        Preconditions.checkNotNull(c);

        this.workerThread = new Thread(this);
        this.raftLog = new RaftLog();
        this.stateMachineJobExecutors = Executors.newFixedThreadPool(8, new ThreadFactoryImpl("state-machine-executors-"));
        this.tickGenerator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("tick-generator-"));

        this.selfId = c.selfId;
        this.tickIntervalMs = c.tickIntervalMs;
        this.suggestElectionTimeoutTicks = c.suggestElectionTimeoutTicks;
        this.pingIntervalTicks = c.pingIntervalTicks;
        this.maxEntriesPerAppend = c.maxEntriesPerAppend;
        this.meta = new RaftPersistentState(c.persistentStateFileDirPath, c.selfId);
        this.stateMachine = new AsyncNotifyStateMachineProxy(c.stateMachine);
        this.broker = c.broker;

        //TODO consider do not include selfId into peerNodes ?
        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1, RaftImpl.this.maxEntriesPerAppend));
        }

        this.state = follower;
    }

    void start() {
        meta.init();
        this.reset(meta.getTerm());

        tickGenerator.scheduleWithFixedDelay(() -> {
            long tick = tickCount.incrementAndGet();
            if (state.isTickTimeout(tick)) {
                clearTickCount();
                markTickerTimeout();
                wakeUpWorker();
            }

        }, tickIntervalMs, tickIntervalMs, TimeUnit.MILLISECONDS);

        workerThread.start();

        logger.info("node {} started with:\n" +
                        "term={}\n" +
                        "votedFor={}\n" +
                        "electionTimeout={}\n" +
                        "tickIntervalMs={}\n" +
                        "pingIntervalTicks={}\n" +
                        "suggectElectionTimeoutTicks={}\n" +
                        "raftLog={}\n",
                this, meta.getTerm(), meta.getVotedFor(), electionTimeoutTicks, tickIntervalMs, pingIntervalTicks,
                suggestElectionTimeoutTicks, raftLog);
    }

    private void markTickerTimeout() {
        tickerTimeout.compareAndSet(false, true);
    }

    private void clearTickCount() {
        tickCount.set(0);
    }

    boolean isLeader() {
        return this.getState() == State.LEADER;
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return peerNodes;
    }

    private int getQuorum() {
        return peerNodes.size() / 2 + 1;
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
        status.setTerm(meta.getTerm());
        status.setCommitIndex(raftLog.getCommitIndex());
        status.setAppliedIndex(raftLog.getAppliedIndex());
        status.setId(this.selfId);
        status.setLeaderId(this.leaderId);
        status.setVotedFor(meta.getVotedFor());
        status.setState(this.getState());
        status.setPeerNodeIds(new ArrayList<>(peerNodes.keySet()));

        return status;
    }

    void appliedTo(int appliedTo) {
        raftLog.appliedTo(appliedTo);
    }

    CompletableFuture<ProposeResponse> propose(List<byte[]> entries) {
        return propose(entries, LogEntry.EntryType.LOG);
    }

    CompletableFuture<ProposeResponse> propose(List<byte[]> entries, LogEntry.EntryType type) {
        String leaderId = this.getLeaderId();
        CompletableFuture<ProposeResponse> future;
        if (this.getState() == State.LEADER) {
            future = new CompletableFuture<>();
            List<LogEntry> logEntries =
                    entries.stream().map(data -> LogEntry.newBuilder()
                            .setData(ByteString.copyFrom(data))
                            .setType(type)
                            .build()).collect(Collectors.toList());
            proposalQueue.add(new Proposal(logEntries, future, type == LogEntry.EntryType.CONFIG));
            wakeUpWorker();
        } else {
            future = CompletableFuture.completedFuture(new ProposeResponse(leaderId, ErrorMsg.NOT_LEADER));
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
            stateMachineJobExecutors.submit(() -> broker.onWriteCommand(builder.build()));
        } catch (RejectedExecutionException ex) {
            logger.error("node {} submit write out command job failed", this, ex);
        }
    }

    void queueReceivedCommand(RaftCommand cmd) {
        if (!peerNodes.containsKey(cmd.getFrom())) {
            logger.warn("node {} received cmd {} from unknown peer", this, cmd);
            return;
        }

        receivedCmdQueue.add(cmd);
    }

    @Override
    public void run() {
        // init state
        RaftImpl.this.state.start();

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
                while ((p = RaftImpl.this.proposalQueue.poll()) != null) {
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
                logger.error("node {} got unexpected exception, self shutdown immediately", this, t);
                shutdown();
            }
        }
    }

    private void processTickTimeout() {
        if (tickerTimeout.compareAndSet(true, false)) {
            try {
                this.state.onTickTimeout();
            } catch (Throwable t) {
                logger.error("process tick timeout failed on node {}", this, t);
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
            cmd = RaftImpl.this.receivedCmdQueue.poll(1, TimeUnit.SECONDS);
            if (cmd != null) {
                cmds = new ArrayList<>();
                cmds.add(cmd);
                initialed = true;
            }
        } catch (InterruptedException ex) {
            processTickTimeout();
        }

        int i = 0;
        while (i < 1000 && (cmd = RaftImpl.this.receivedCmdQueue.poll()) != null) {
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
        final int selfTerm = meta.getTerm();
        if (cmd.getType() == RaftCommand.CmdType.REQUEST_VOTE) {
            if (leaderId != null && tickCount.get() < electionTimeoutTicks) {
                // if a server receives a RequestVote request within the minimum election timeout of hearing
                // from a current leader, it does not update its term or grant its vote
                logger.info("node {} ignore request vote cmd: {} because it's leader still valid. ticks remain: {}",
                        this, cmd, electionTimeoutTicks - tickCount.get());
                return;
            }


            logger.debug("node {} received request vote command, request={}", this, cmd);
            final String candidateId = cmd.getFrom();
            boolean isGranted = false;

            // each server will vote for at most one candidate in a given term, on a first-come-first-served basis
            // so we only need to check votedFor when term in this command equals to the term of this raft server
            final String votedFor = meta.getVotedFor();
            if ((cmd.getTerm() > selfTerm || (cmd.getTerm() == selfTerm && (votedFor == null || votedFor.equals(candidateId))))
                    && this.raftLog.isUpToDate(cmd.getLastLogTerm(), cmd.getLastLogIndex())) {
                isGranted = true;
                this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                meta.setVotedFor(candidateId);
            }

            RaftCommand.Builder resp = RaftCommand.newBuilder()
                    .setType(RaftCommand.CmdType.REQUEST_VOTE_RESP)
                    .setVoteGranted(isGranted)
                    .setTo(cmd.getFrom())
                    .setTerm(cmd.getTerm());
            writeOutCommand(resp);
        } else {
            // check term
            if (cmd.getTerm() < selfTerm) {
                RaftCommand.CmdType respType = null;
                RaftCommand.Builder resp = RaftCommand.newBuilder()
                        .setTo(cmd.getFrom())
                        .setSuccess(false)
                        .setTerm(selfTerm);

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

            if (cmd.getTerm() > selfTerm) {
                this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
            }

            state.process(cmd);
        }
    }

    private void processProposal(Proposal proposal) {
        ErrorMsg error = null;
        try {
            if (this.getState() == State.LEADER) {
                if (existsPendingConfigChange && proposal.isContainsConfigChange) {
                    error = ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION;
                } else {
                    if (proposal.isContainsConfigChange) {
                        existsPendingConfigChange = true;
                    }
                    final int term = meta.getTerm();
                    this.raftLog.directAppend(term, proposal.entries);
                    this.broadcastAppendEntries();
                }
            } else {
                error = ErrorMsg.NOT_LEADER;
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
            final int selfTerm = meta.getTerm();
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(this.selfId)) {
                    peer.sendAppend(selfTerm);
                }
            }
        }
    }

    private void writeOutCommitedLogs(List<LogEntry> commitedLogs) {
        assert commitedLogs != null;
        if (! commitedLogs.isEmpty()) {
            for (LogEntry e : commitedLogs) {
                if (e.getType() == LogEntry.EntryType.CONFIG) {
                    try {
                        ConfigChange change = ConfigChange.parseFrom(e.getData());
                        String peerId = change.getPeerId();
                        switch (change.getAction()) {
                            case ADD_NODE:
                                addNode(peerId);
                                break;
                            case REMOVE_NODE:
                                removeNode(peerId);
                                break;
                            default:
                                String errorMsg = String.format("node %s got unrecognized change configuration action: %s",
                                        this, e);
                                throw new RuntimeException(errorMsg);

                        }
                    } catch (InvalidProtocolBufferException ex) {
                        String errorMsg = String.format("node %s failed to parse ConfigChange msg: %s", this, e);
                        throw new RuntimeException(errorMsg, ex);
                    }
                }
            }

            stateMachine.onProposalCommitted(commitedLogs);
        }
    }

    private void addNode(final String peerId) {
        if (! peerNodes.containsKey(peerId)) {
            peerNodes.computeIfAbsent(peerId,
                    k -> new RaftPeerNode(peerId,
                            this,
                            this.raftLog,
                            this.raftLog.getLastIndex() + 1,
                            RaftImpl.this.maxEntriesPerAppend));

            logger.info("{} add peerId \"{}\" to cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
            stateMachine.onNodeAdded(peerId);
        }

        existsPendingConfigChange = false;
    }

    private void removeNode(final String peerId) {
        if (peerNodes.remove(peerId) != null) {
            logger.info("{} remove peerId \"{}\" from cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
            stateMachine.onNodeRemoved(peerId);

            // quorum has changed, check if there's any pending entries
            if (getState() == State.LEADER && updateCommit()) {
                broadcastAppendEntries();
            }
        }

        existsPendingConfigChange = false;
    }

    private boolean updateCommit() {
        // kth biggest number
        int k = RaftImpl.this.getQuorum() - 1;
        List<Integer> matchedIndexes = peerNodes.values().stream()
                .map(RaftPeerNode::getMatchIndex).sorted().collect(Collectors.toList());
        int kthMatchedIndexes = matchedIndexes.get(k);
        Optional<LogEntry> kthLog = RaftImpl.this.raftLog.getEntry(kthMatchedIndexes);
        if (kthLog.isPresent()) {
            LogEntry e = kthLog.get();
            // this is a key point. Raft never commits log entries from previous terms by counting replicas
            // Only log entries from the leader’s current term are committed by counting replicas; once an entry
            // from the current term has been committed in this way, then all prior entries are committed
            // indirectly because of the Log Matching Property
            if (e.getTerm() == RaftImpl.this.meta.getTerm()) {
                RaftImpl.this.raftLog.tryCommitTo(kthMatchedIndexes);
                writeOutCommitedLogs(RaftImpl.this.raftLog.getEntriesNeedToApply());
                return true;
            }
        }
        return false;
    }

    private void broadcastPing() {
        final int selfTerm = meta.getTerm();
        for (final RaftPeerNode peer : peerNodes.values()) {
            if (!peer.getPeerId().equals(this.selfId)) {
                peer.sendPing(selfTerm);
            }
        }
    }

    private boolean tryBecomeFollower(int term, String leaderId) {
        assert Thread.currentThread() == workerThread;

        final int selfTerm = meta.getTerm();
        if (term >= selfTerm) {
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
            this.reset(meta.getTerm());
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

        RaftImpl.this.reset(meta.getTerm());
        RaftImpl.this.transitState(RaftImpl.this.candidate);

        return true;
    }

    private void reset(int term) {
        // reset votedFor only when term changed
        // so when a candidate transit to leader it can keep votedFor to itself then when it receives
        // a request vote with the same term, it can reject that request
        if (meta.getTerm() != term) {
            meta.setTermAndVotedFor(term, null);
        }

        this.leaderId = null;
        this.clearTickCount();
        this.tickerTimeout.getAndSet(false);

        // we need to reset election timeout on every time state changed and every
        // reelection in candidate state to avoid split vote
        this.electionTimeoutTicks = RaftImpl.generateElectionTimeoutTicks(RaftImpl.this.suggestElectionTimeoutTicks);
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
                .setTerm(meta.getTerm())
                .setSuccess(false);
        if (cmd.getPrevLogIndex() < raftLog.getCommitIndex()) {
            resp.setMatchIndex(raftLog.getCommitIndex());
        } else {
            int matchIndex = RaftImpl.this.replicateLogsOnFollower(cmd);
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
                .setTerm(meta.getTerm());
        raftLog.tryCommitTo(cmd.getLeaderCommit());
        pong.setSuccess(true);
        writeOutCommand(pong);
        writeOutCommitedLogs(raftLog.getEntriesNeedToApply());
    }

    void shutdown() {
        logger.info("shutting down node {} ...", this);
        tickGenerator.shutdown();
        stateMachineJobExecutors.shutdown();
        workerRun = false;
        wakeUpWorker();
        stateMachine.onShutdown();
        logger.info("node {} shutdown", this);
    }

    @Override
    public String toString() {
        return "{" +
                "term=" + meta.getTerm() +
                ", id='" + selfId + '\'' +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + this.getState() +
                '}';
    }

    private static class Proposal {
        private final List<LogEntry> entries;
        private final CompletableFuture<ProposeResponse> future;
        private final boolean isContainsConfigChange;

        Proposal(List<LogEntry> entries, CompletableFuture<ProposeResponse> future, boolean isContainsConfigChange) {
            this.entries = new ArrayList<>(entries);
            this.future = future;
            this.isContainsConfigChange = isContainsConfigChange;
        }
    }

    private class Leader extends RaftState {
        Leader() {
            super(State.LEADER);
        }

        public void start() {
            logger.debug("node {} start leader", RaftImpl.this);
            RaftImpl.this.broadcastPing();
        }

        public void finish() {
            logger.debug("node {} finish leader", RaftImpl.this);
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftImpl.this.pingIntervalTicks;
        }

        @Override
        public void onTickTimeout() {
            RaftImpl.this.broadcastPing();
        }

        @Override
        void process(RaftCommand cmd) {
            final int selfTerm = RaftImpl.this.meta.getTerm();
            switch (cmd.getType()) {
                case APPEND_ENTRIES_RESP:
                    if (cmd.getTerm() > selfTerm) {
                        assert !cmd.getSuccess();
                        tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    } else if (cmd.getTerm() == selfTerm) {
                        RaftPeerNode node = peerNodes.get(cmd.getFrom());

                        assert node != null;

                        if (cmd.getSuccess()) {
                            if (node.updateIndexes(cmd.getMatchIndex()) && updateCommit()) {
                                broadcastAppendEntries();
                            }
                        } else {
                            node.decreaseIndexAndResendAppend(selfTerm);
                        }
                    }
                    break;
                case PONG:
                    // resend pending append entries
                    RaftPeerNode node = peerNodes.get(cmd.getFrom());
                    if (node.getMatchIndex() < RaftImpl.this.raftLog.getLastIndex()) {
                        node.sendAppend(selfTerm);
                    }
                    break;
                case REQUEST_VOTE_RESP:
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }
    }

    private class Follower extends RaftState {
        Follower() {
            super(State.FOLLOWER);
        }

        public void start() {
            logger.debug("node {} start follower", RaftImpl.this);
        }

        public void finish() {
            logger.debug("node {} finish follower", RaftImpl.this);
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftImpl.this.electionTimeoutTicks;
        }

        @Override
        public void onTickTimeout() {
            logger.info("election timeout, node {} become candidate", RaftImpl.this);

            RaftImpl.this.tryBecomeCandidate();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES:
                    RaftImpl.this.clearTickCount();
                    RaftImpl.this.leaderId = cmd.getLeaderId();
                    RaftImpl.this.processAppendEntries(cmd);
                    break;
                case PING:
                    RaftImpl.this.clearTickCount();
                    RaftImpl.this.leaderId = cmd.getFrom();
                    RaftImpl.this.processHeartbeat(cmd);
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }
    }

    private class Candidate extends RaftState {
        private volatile ConcurrentHashMap<String, Boolean> votesGot = new ConcurrentHashMap<>();

        Candidate() {
            super(State.CANDIDATE);
        }

        public void start() {
            logger.debug("node {} start candidate", RaftImpl.this);
            this.startElection();
        }

        public void finish() {
            logger.debug("node {} finish candidate", RaftImpl.this);
        }

        private void startElection() {
            votesGot = new ConcurrentHashMap<>();
            RaftImpl.this.meta.setVotedFor(RaftImpl.this.selfId);
            assert RaftImpl.this.getState() == State.CANDIDATE;

            int oldTerm = RaftImpl.this.meta.getTerm();
            final int candidateTerm = oldTerm + 1;
            RaftImpl.this.meta.setTerm(candidateTerm);

            // got self vote initially
            final int votesToWin = RaftImpl.this.getQuorum() - 1;

            if (votesToWin == 0) {
                RaftImpl.this.tryBecomeLeader();
            } else {
                Optional<LogEntry> lastEntry = RaftImpl.this.raftLog.getEntry(RaftImpl.this.raftLog.getLastIndex());
                assert lastEntry.isPresent();

                LogEntry e = lastEntry.get();

                logger.debug("node {} start election, votesToWin={}", RaftImpl.this, votesToWin);
                for (final RaftPeerNode node : RaftImpl.this.getPeerNodes().values()) {
                    if (!node.getPeerId().equals(RaftImpl.this.selfId)) {
                        RaftCommand vote = RaftCommand.newBuilder()
                                .setType(RaftCommand.CmdType.REQUEST_VOTE)
                                .setTerm(candidateTerm)
                                .setFrom(RaftImpl.this.selfId)
                                .setLastLogIndex(e.getIndex())
                                .setLastLogTerm(e.getTerm())
                                .setTo(node.getPeerId())
                                .build();
                        broker.onWriteCommand(vote);
                    }
                }
            }
        }

        public void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case REQUEST_VOTE_RESP:
                    logger.debug("node {} received request vote response={}", RaftImpl.this, cmd);

                    if (cmd.getTerm() == RaftImpl.this.meta.getTerm()) {
                        if (cmd.getVoteGranted()) {
                            final int votesToWin = RaftImpl.this.getQuorum() - 1;
                            votesGot.put(cmd.getFrom(), true);
                            int votes = votesGot.values().stream().mapToInt(isGrunt -> isGrunt ? 1 : 0).sum();
                            if (votes >= votesToWin) {
                                RaftImpl.this.tryBecomeLeader();
                            }
                        }
                    }
                    break;
                case APPEND_ENTRIES:
                    // if term in cmd is greater than current term we are already transferred to follower
                    RaftImpl.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    RaftImpl.this.processAppendEntries(cmd);
                    break;
                case PING:
                    RaftImpl.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    RaftImpl.this.processHeartbeat(cmd);
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftImpl.this.electionTimeoutTicks;
        }

        @Override
        public void onTickTimeout() {
            RaftImpl.this.tryBecomeCandidate();
        }
    }
}
