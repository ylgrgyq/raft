package raft.server;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
import raft.server.proto.ConfigChange;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;
import raft.server.proto.RaftCommand;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftImpl implements Raft {
    private static final Logger logger = LoggerFactory.getLogger(RaftImpl.class.getName());

    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong electionTickCounter = new AtomicLong();
    private final AtomicBoolean electionTickerTimeout = new AtomicBoolean();
    private final AtomicBoolean pendingUpdateCommit = new AtomicBoolean();
    private final AtomicLong pingTickCounter = new AtomicLong();
    private final AtomicBoolean pingTickerTimeout = new AtomicBoolean();
    private final AtomicBoolean wakenUp = new AtomicBoolean();
    private final BlockingQueue<Proposal> proposalQueue = new LinkedBlockingQueue<>(1000);
    private final BlockingQueue<RaftCommand> receivedCmdQueue = new LinkedBlockingQueue<>(1000);
    private final PendingProposalFutures pendingProposal = new PendingProposalFutures();

    private final Config c;
    private final ScheduledExecutorService tickGenerator;
    private final String selfId;
    private final RaftLog raftLog;
    private final RaftPersistentMeta meta;
    private final StateMachineProxy stateMachine;
    private final AsyncRaftCommandBrokerProxy broker;
    private final AtomicBoolean started = new AtomicBoolean(false);

    private volatile String leaderId;
    private RaftState state;
    private long electionTimeoutTicks;
    private Thread workerThread;
    private volatile boolean workerRun = true;
    private boolean existsPendingConfigChange = false;
    private TransferLeaderFuture transferLeaderFuture = null;

    public RaftImpl(Config c) {
        Preconditions.checkNotNull(c);

        this.c = c;
        this.workerThread = new Thread(new Worker());
        this.raftLog = new RaftLogImpl(c.storage);
        this.tickGenerator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("tick-generator-"));

        this.selfId = c.selfId;
        this.meta = new RaftPersistentMeta(c.persistentMetaFileDirPath, c.selfId, c.syncWriteStateFile);
        this.stateMachine = new StateMachineProxy(c.stateMachine, this.raftLog);
        this.broker = new AsyncRaftCommandBrokerProxy(c.broker);

        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1, c.maxEntriesPerAppend));
        }

        this.state = follower;
    }

    @Override
    public Raft start() {
        if (!started.compareAndSet(false, true)) {
            return this;
        }

        meta.init();
        reset(meta.getTerm());

        raftLog.init(meta);
        if (c.appliedTo > -1) {
            raftLog.appliedTo(c.appliedTo);
        }

        tickGenerator.scheduleWithFixedDelay(() -> {
            boolean wakeup = false;
            if (electionTickCounter.incrementAndGet() >= electionTimeoutTicks) {
                electionTickCounter.set(0L);
                electionTickerTimeout.compareAndSet(false, true);
                wakeup = true;
            }

            if (pingTickCounter.incrementAndGet() >= c.pingIntervalTicks) {
                pingTickCounter.set(0L);
                pingTickerTimeout.compareAndSet(false, true);
                wakeup = true;
            }

            if (wakeup) {
                wakeUpWorker();
            }

        }, c.tickIntervalMs, c.tickIntervalMs, TimeUnit.MILLISECONDS);

        workerThread.start();

        logger.info("node {} started with:\n" +
                        "term={}\n" +
                        "votedFor={}\n" +
                        "electionTimeout={}\n" +
                        "tickIntervalMs={}\n" +
                        "pingIntervalTicks={}\n" +
                        "suggectElectionTimeoutTicks={}\n" +
                        "raftLog={}\n",
                this, meta.getTerm(), meta.getVotedFor(), electionTimeoutTicks, c.tickIntervalMs, c.pingIntervalTicks,
                c.suggestElectionTimeoutTicks, raftLog);
        return this;
    }

    @Override
    public String getId() {
        return selfId;
    }

    private State getState() {
        return state.getState();
    }

    private RaftStatusSnapshot getStatus() {
        assert Thread.currentThread() == workerThread;

        RaftStatusSnapshot status = new RaftStatusSnapshot();
        status.setTerm(meta.getTerm());
        status.setCommitIndex(raftLog.getCommitIndex());
        status.setAppliedIndex(raftLog.getAppliedIndex());
        status.setLeaderId(leaderId);
        status.setState(getState());
        status.setPeerNodeIds(new ArrayList<>(peerNodes.keySet()));

        return status;
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return peerNodes;
    }

    private int getQuorum() {
        return peerNodes.size() / 2 + 1;
    }

    String getLeaderId() {
        return leaderId;
    }

    Optional<LogSnapshot> getRecentSnapshot(long expectIndex) {
        return stateMachine.getRecentSnapshot(expectIndex);
    }

    @Override
    public CompletableFuture<ProposalResponse> addNode(String newNode) {
        checkNotNull(newNode);
        checkArgument(!newNode.isEmpty());
        checkState(started.get(), "raft server not start or already shutdown");

        return proposeConfigChange(newNode, ConfigChange.ConfigChangeAction.ADD_NODE);
    }

    @Override
    public CompletableFuture<ProposalResponse> removeNode(String newNode) {
        checkNotNull(newNode);
        checkArgument(!newNode.isEmpty());
        checkState(started.get(), "raft server not start or already shutdown");

        if (newNode.equals(getLeaderId())) {
            return CompletableFuture.completedFuture(
                    ProposalResponse.errorWithLeaderHint(getLeaderId(), ErrorMsg.FORBID_REMOVE_LEADER));
        }

        return proposeConfigChange(newNode, ConfigChange.ConfigChangeAction.REMOVE_NODE);
    }

    private CompletableFuture<ProposalResponse> proposeConfigChange(final String peerId, final ConfigChange.ConfigChangeAction action) {
        ConfigChange change = ConfigChange.newBuilder()
                .setAction(action)
                .setPeerId(peerId)
                .build();

        ArrayList<byte[]> data = new ArrayList<>();
        data.add(change.toByteArray());

        return doPropose(data, LogEntry.EntryType.CONFIG);
    }

    @Override
    public CompletableFuture<ProposalResponse> transferLeader(String transfereeId) {
        checkNotNull(transfereeId);
        checkArgument(!transfereeId.isEmpty());
        checkState(started.get(), "raft server not start or already shutdown");

        ArrayList<byte[]> data = new ArrayList<>();
        data.add(transfereeId.getBytes(StandardCharsets.UTF_8));

        return doPropose(data, LogEntry.EntryType.TRANSFER_LEADER);
    }

    @Override
    public CompletableFuture<ProposalResponse> propose(List<byte[]> data) {
        checkNotNull(data);
        checkArgument(!data.isEmpty());
        checkState(started.get(), "raft server not start or already shutdown");

        logger.debug("node {} receives proposal with {} entries", this, data.size());

        return doPropose(data, LogEntry.EntryType.LOG);
    }

    private CompletableFuture<ProposalResponse> doPropose(final List<byte[]> datas, final LogEntry.EntryType type) {
        List<LogEntry> logEntries = datas
                .stream()
                .map(d -> LogEntry.newBuilder()
                        .setData(ByteString.copyFrom(d))
                        .setType(type)
                        .build()).collect(Collectors.toList());

        Proposal proposal = new Proposal(logEntries, type);
        CompletableFuture<ProposalResponse> future;
        if (getState() == State.LEADER) {
            future = proposal.getFuture();
            proposalQueue.add(proposal);
            wakeUpWorker();
        } else {
            future = CompletableFuture.completedFuture(ProposalResponse.errorWithLeaderHint(leaderId, ErrorMsg.NOT_LEADER));
        }
        return future;
    }

    private void wakeUpWorker() {
        if (wakenUp.compareAndSet(false, true)) {
            workerThread.interrupt();
        }
    }

    void writeOutCommand(RaftCommand.Builder builder) {
        builder.setFrom(selfId);
        broker.onWriteCommand(builder.build());
    }

    @Override
    public void receiveCommand(RaftCommand cmd) {
        checkNotNull(cmd);

        if (!peerNodes.containsKey(cmd.getFrom())) {
            logger.warn("node {} received cmd {} from unknown peer", this, cmd);
            return;
        }

        receivedCmdQueue.add(cmd);
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            // init state
            state.start(new Context());

            while (workerRun) {
                try {
                    processTickTimeout();

                    processPendingUpdateCommit();

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
                    panic("unexpected exception", t);
                }
            }
        }
    }

    private void processTickTimeout() {
        if (electionTickerTimeout.compareAndSet(true, false)) {
            state.onElectionTimeout();
        }

        if (pingTickerTimeout.compareAndSet(true, false)) {
            state.onPingTimeout();
        }
    }

    private void processPendingUpdateCommit() {
        if (pendingUpdateCommit.compareAndSet(true, false)) {
            updateCommit();
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

        // we must set wake up mark to prevent receiving interrupt during block writing persistent state to file
        // because if that happens we will get an unexpected java.nio.channels.ClosedByInterruptException
        wakenUp.getAndSet(true);
        // clear interrupt mark in case of it has set
        Thread.interrupted();

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
        final long selfTerm = meta.getTerm();
        if (cmd.getType() == RaftCommand.CmdType.REQUEST_VOTE) {
            if (!cmd.getForceElection() && leaderId != null && electionTickCounter.get() < electionTimeoutTicks) {
                // if a server receives a RequestVote request within the minimum election timeout of hearing
                // from a current leader, it does not update its term or grant its vote except the force election mark
                // is set which indicate that this REQUEST_VOTE command is from a legitimate server during leader
                // transfer operation
                logger.info("node {} ignore request vote cmd: {} because it's leader still valid. ticks remain: {}",
                        this, cmd, electionTimeoutTicks - electionTickCounter.get());
                return;
            }


            logger.debug("node {} received request vote command, request={}", this, cmd);
            final String candidateId = cmd.getFrom();
            boolean isGranted = false;

            // each server will vote for at most one candidate in a given term, on a first-come-first-served basis
            // so we only need to check votedFor when term in this command equals to the term of this raft server
            final String votedFor = meta.getVotedFor();
            if ((cmd.getTerm() > selfTerm || (cmd.getTerm() == selfTerm && (votedFor == null || votedFor.equals(candidateId))))
                    && raftLog.isUpToDate(cmd.getLastLogTerm(), cmd.getLastLogIndex())) {
                isGranted = true;
                tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                logger.debug("node {} set voted for {}", this, candidateId);
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
                tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
            }

            state.process(cmd);
        }
    }

    private void processProposal(Proposal proposal) {
        logger.debug("node {} start process [{}]", this, proposal);
        ErrorMsg error = null;
        try {
            if (getState() == State.LEADER) {
                if (transferLeaderFuture != null) {
                    error = ErrorMsg.LEADER_TRANSFERRING;
                } else {
                    switch (proposal.getType()) {
                        case CONFIG:
                            if (existsPendingConfigChange) {
                                error = ErrorMsg.EXISTS_UNAPPLIED_CONFIGURATION;
                                break;
                            }

                            existsPendingConfigChange = true;
                            // fall through
                        case LOG:
                            final long term = meta.getTerm();
                            long newLastIndex = raftLog.leaderAsyncAppend(term, proposal.getEntries(), (lastIndex, err) -> {
                                if (err != null) {
                                    panic("async append logs failed", err);
                                } else {
                                    logger.debug("node {} try update index to {}", this, lastIndex);
                                    // it's OK if this node has stepped-down and surrendered leadership before we
                                    // updated index because we don't use RaftPeerNode on follower or candidate
                                    RaftPeerNode leaderNode = peerNodes.get(selfId);
                                    if (leaderNode != null) {
                                        if (leaderNode.updateIndexes(lastIndex)) {
                                            pendingUpdateCommit.compareAndSet(false, true);
                                        }
                                    } else {
                                        logger.error("node {} was removed from remote peer set {}",
                                                this, peerNodes.keySet());
                                    }
                                }
                            });

                            pendingProposal.addFuture(newLastIndex, proposal.getFuture());
                            broadcastAppendEntries();
                            return;
                        case TRANSFER_LEADER:
                            String transereeId = new String(proposal.getEntries().get(0).getData().toByteArray(), StandardCharsets.UTF_8);

                            if (transereeId.equals(selfId)) {
                                error = ErrorMsg.ALLREADY_LEADER;
                                break;
                            }

                            if (!peerNodes.containsKey(transereeId)) {
                                error = ErrorMsg.UNKNOWN_TRANSFEREEID;
                                break;
                            }

                            logger.info("node {} start transfer leadership to {}", this, transereeId);

                            electionTickCounter.set(0);
                            transferLeaderFuture = new TransferLeaderFuture(transereeId, proposal.getFuture());
                            RaftPeerNode n = peerNodes.get(transereeId);
                            if (n.getMatchIndex() == raftLog.getLastIndex()) {
                                logger.info("node {} send timeout immediately to {} because it already " +
                                        "has up to date logs", this, transereeId);
                                n.sendTimeout(meta.getTerm());
                            } else {
                                n.sendAppend(meta.getTerm());
                            }

                            return;
                    }
                }
            } else {
                error = ErrorMsg.NOT_LEADER;
            }
        } catch (Throwable t) {
            logger.error("process propose failed on node {}", this, t);
            error = ErrorMsg.INTERNAL_ERROR;
        }

        proposal.getFuture().complete(ProposalResponse.errorWithLeaderHint(leaderId, error));
    }

    private void broadcastAppendEntries() {
        if (peerNodes.size() == 1) {
            tryCommitTo(raftLog.getLastIndex());
        } else {
            final long selfTerm = meta.getTerm();
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(selfId)) {
                    peer.sendAppend(selfTerm);
                }
            }
        }
    }

    private CompletableFuture<Void> processNewCommitedLogs(List<LogEntry> commitedLogs) {
        assert commitedLogs != null;
        assert !commitedLogs.isEmpty();

        List<LogEntry> withoutConfigLogs = new ArrayList<>(commitedLogs.size());
        for (LogEntry e : commitedLogs) {
            if (e.getType() == LogEntry.EntryType.CONFIG) {
                try {
                    ConfigChange change = ConfigChange.parseFrom(e.getData());
                    String peerId = change.getPeerId();
                    switch (change.getAction()) {
                        case ADD_NODE:
                            addNode0(peerId);
                            break;
                        case REMOVE_NODE:
                            removeNode0(peerId);
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
            } else {
                withoutConfigLogs.add(e);
            }
        }

        LogEntry lastLog = commitedLogs.get(commitedLogs.size() - 1);
        return stateMachine.onProposalCommitted(getStatus(), withoutConfigLogs, lastLog.getIndex());
    }

    private void addNode0(final String peerId) {
        peerNodes.computeIfAbsent(peerId,
                k -> new RaftPeerNode(peerId,
                        this,
                        raftLog,
                        raftLog.getLastIndex() + 1,
                        c.maxEntriesPerAppend));

        logger.info("node {} add peerId \"{}\" to cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
        stateMachine.onNodeAdded(getStatus(), peerId);

        existsPendingConfigChange = false;
    }

    private void removeNode0(final String peerId) {
        // TODO if selfId was removed from peerNodes
        peerNodes.remove(peerId);

        logger.info("node {} remove peerId \"{}\" from cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
        stateMachine.onNodeRemoved(getStatus(), peerId);

        if (getState() == State.LEADER) {
            // abort transfer leadership when transferee was removed from cluster
            if (transferLeaderFuture != null && peerId.equals(transferLeaderFuture.getTransfereeId())) {
                transferLeaderFuture.getResponseFuture()
                        .complete(ProposalResponse.error(ErrorMsg.TRANSFER_ABORTED_BY_TRANSFEREE_REMOVED));
                transferLeaderFuture = null;
            }

            // quorum has changed, check if there's any pending entries
            if (updateCommit()) {
                broadcastAppendEntries();
            }
        }

        existsPendingConfigChange = false;
    }

    private boolean updateCommit() {
        assert getState() == State.LEADER;

        // kth biggest number
        int k = getQuorum() - 1;
        List<Long> matchedIndexes = peerNodes.values().stream()
                .map(RaftPeerNode::getMatchIndex).sorted().collect(Collectors.toList());
        logger.info("node {} update commit with peerNodes {}", this, peerNodes);
        long kthMatchedIndexes = matchedIndexes.get(k);
        if (kthMatchedIndexes >= raftLog.getFirstIndex()) {
            Optional<LogEntry> kthLog = raftLog.getEntry(kthMatchedIndexes);
            if (kthLog.isPresent()) {
                LogEntry e = kthLog.get();
                // this is a key point. Raft never commits log entries from previous terms by counting replicas
                // Only log entries from the leader’s current term are committed by counting replicas; once an entry
                // from the current term has been committed in this way, then all prior entries are committed
                // indirectly because of the Log Matching Property
                if (e.getTerm() == meta.getTerm()) {
                    tryCommitTo(kthMatchedIndexes);
                    return true;
                }
            }
        }
        return false;
    }

    private void broadcastPing() {
        System.out.println("broadcast ping to " + peerNodes);
        final long selfTerm = meta.getTerm();
        if (peerNodes.size() == 1) {
            tryCommitTo(raftLog.getLastIndex());
        } else {
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(selfId)) {
                    peer.sendPing(selfTerm);
                }
            }
        }
    }

    private void tryBecomeFollower(long term, String leaderId) {
        assert Thread.currentThread() == workerThread;

        final long selfTerm = meta.getTerm();
        if (term >= selfTerm) {
            transitState(follower, term, leaderId);
        } else {
            logger.error("node {} transient state to {} failed, term = {}, leaderId = {}",
                    this, State.FOLLOWER, term, leaderId);
        }
    }

    private void tryBecomeLeader() {
        assert Thread.currentThread() == workerThread;

        if (getState() == State.CANDIDATE) {
            transitState(leader, meta.getTerm(), selfId);

            // reinitialize nextIndex for every peer node
            // then send them initial ping on start leader state
            for (RaftPeerNode node : peerNodes.values()) {
                node.reset(raftLog.getLastIndex() + 1);
            }
        } else {
            logger.error("node {} transient state to {} failed", this, State.LEADER);
        }
    }

    private void tryBecomeCandidate() {
        assert Thread.currentThread() == workerThread;

        transitState(candidate, meta.getTerm(), null);
    }

    private void reset(long term) {
        // reset votedFor only when term changed
        // so when a candidate transit to leader it can keep votedFor to itself then when it receives
        // a request vote with the same term, it can reject that request
        if (meta.getTerm() != term) {
            meta.setTermAndVotedFor(term, null);
        }

        leaderId = null;

        // I think we don't have any scenario that need to reset transferLeaderFuture to null
        // but for safety we add an assert and reset it to null anyway
        assert transferLeaderFuture == null;
        transferLeaderFuture = null;

        clearTickCounters();
        electionTickerTimeout.getAndSet(false);
        pingTickerTimeout.getAndSet(false);

        // we need to reset election timeout on every time state changed and every
        // reelection in candidate state to avoid split vote
        electionTimeoutTicks = RaftImpl.generateElectionTimeoutTicks(c.suggestElectionTimeoutTicks);
    }

    private void clearTickCounters() {
        electionTickCounter.set(0);
        pingTickCounter.set(0);
    }

    private static long generateElectionTimeoutTicks(long suggestElectionTimeoutTicks) {
        return suggestElectionTimeoutTicks + ThreadLocalRandom.current().nextLong(suggestElectionTimeoutTicks);
    }

    private void transitState(RaftState nextState, long newTerm, String newLeaderId) {
        assert Thread.currentThread() == workerThread;

        // we'd better finish old state before reset term and set leader id. this can insure that old state
        // finished with the old term and leader id while new state started with new term and new leader id
        Context cxt = state.finish();
        state = nextState;
        reset(newTerm);
        if (newLeaderId != null) {
            this.leaderId = newLeaderId;
        }
        nextState.start(cxt);
    }

    private void processAppendEntries(RaftCommand cmd) {
        RaftCommand.Builder resp = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES_RESP)
                .setTo(cmd.getFrom())
                .setTerm(meta.getTerm())
                .setSuccess(false);
        if (cmd.getPrevLogIndex() < raftLog.getCommitIndex()) {
            resp = resp.setMatchIndex(raftLog.getCommitIndex()).setSuccess(true);
        } else {
            long matchIndex = RaftImpl.this.replicateLogsOnFollower(cmd);
            if (matchIndex != -1L) {
                tryCommitTo(Math.min(cmd.getLeaderCommit(), matchIndex));

                resp.setSuccess(true);
                resp.setMatchIndex(matchIndex);
            }
        }
        writeOutCommand(resp);
    }

    private long replicateLogsOnFollower(RaftCommand cmd) {
        long prevIndex = cmd.getPrevLogIndex();
        long prevTerm = cmd.getPrevLogTerm();
        String leaderId = cmd.getLeaderId();
        List<raft.server.proto.LogEntry> entries = cmd.getEntriesList();

        State state = getState();
        if (state == State.FOLLOWER) {
            try {
                this.leaderId = leaderId;
                return raftLog.followerSyncAppend(prevIndex, prevTerm, entries);
            } catch (Exception ex) {
                logger.error("append entries failed on node {}, leaderId {}, entries {}",
                        this, leaderId, entries, ex);
            }
        } else {
            logger.error("append logs failed on node {} due to invalid state: {}", this, state);
        }

        return -1;
    }

    private void processHeartbeat(RaftCommand cmd) {
        RaftCommand.Builder pong = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.PONG)
                .setTo(cmd.getFrom())
                .setSuccess(true)
                .setTerm(meta.getTerm());
        tryCommitTo(cmd.getLeaderCommit());
        writeOutCommand(pong);
    }

    private void tryCommitTo(long commitTo) {
        if (logger.isDebugEnabled()) {
            logger.debug("node {} try commit to {} with current commitIndex: {} and lastIndex: {}",
                    this, commitTo, raftLog.getCommitIndex(), raftLog.getLastIndex());
        }

        List<LogEntry> newCommitedLogs = raftLog.tryCommitTo(commitTo);
        if (!newCommitedLogs.isEmpty()) {
            LogEntry last = newCommitedLogs.get(newCommitedLogs.size() - 1);
            assert last.getIndex() <= commitTo;
            meta.setCommitIndex(last.getIndex());
            processNewCommitedLogs(newCommitedLogs)
                    .whenComplete((ret, t) ->
                            pendingProposal.completeFutures(last.getIndex())
                    );
        }
    }

    private void processSnapshot(RaftCommand cmd) {
        RaftCommand.Builder resp = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES_RESP)
                .setTo(cmd.getFrom())
                .setTerm(meta.getTerm())
                .setSuccess(true);

        // apply snapshot
        if (tryApplySnapshot(cmd.getSnapshot())) {
            logger.info("node {} install snapshot success, update matching index to {}", this, raftLog.getLastIndex());
            resp.setMatchIndex(raftLog.getLastIndex());
        } else {
            logger.info("node {} install snapshot failed, update matching index to {}", this, raftLog.getCommitIndex());
            resp.setMatchIndex(raftLog.getCommitIndex());
        }

        writeOutCommand(resp);
    }

    private boolean tryApplySnapshot(LogSnapshot snapshot) {
        if (snapshot.getIndex() <= raftLog.getCommitIndex()) {
            return false;
        }

        if (raftLog.match(snapshot.getIndex(), snapshot.getTerm())) {
            logger.info("node {} fast forward commit index to {} due to receive snapshot", this, snapshot.getIndex());
            tryCommitTo(snapshot.getIndex());
            return false;
        }

        logger.info("node {} installing snapshot with index {}, term {}, peerIds {}",
                this, snapshot.getIndex(), snapshot.getTerm(), snapshot.getPeerIdsList());

        raftLog.installSnapshot(snapshot);
        stateMachine.installSnapshot(getStatus(), snapshot);

        long lastIndex = raftLog.getLastIndex();
        Set<String> removedPeerIds = new HashSet<>(peerNodes.keySet());

        for (String peerId : snapshot.getPeerIdsList()) {
            RaftPeerNode node = new RaftPeerNode(peerId, this, raftLog, lastIndex + 1,
                    c.maxEntriesPerAppend);
            if (peerId.equals(selfId)) {
                node.updateIndexes(lastIndex - 1);
            }
            peerNodes.put(peerId, node);
            removedPeerIds.remove(peerId);
        }

        for (String id : removedPeerIds) {
            peerNodes.remove(id);
        }

        return true;
    }

    private void panic(String reason, Throwable err) {
        logger.error("node {} panic due to {}, shutdown immediately", this, reason, err);
        shutdown();
    }

    @Override
    public void shutdown() {
        if (!started.compareAndSet(true, false)) {
            return;
        }

        logger.info("shutting down node {} ...", this);
        tickGenerator.shutdown();
        workerRun = false;
        wakeUpWorker();
        try {
            workerThread.join();
        } catch (InterruptedException ex) {
            // ignore then continue shutdown
        }
        broker.shutdown();
        // we can call onShutdown and shutdown on stateMachine successively
        // shutdown will wait onShutdown to finish
        stateMachine.onShutdown();
        stateMachine.shutdown();
        raftLog.shutdown();
        logger.info("node {} shutdown", this);
    }

    @Override
    public String toString() {
        return "{" +
                "term=" + meta.getTerm() +
                ", id='" + selfId + '\'' +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + getState() +
                ", votedFor='" + meta.getVotedFor() + '\'' +
                '}';
    }

    static class Context {
        boolean receiveTimeoutOnTransferLeader = false;
    }

    private class Leader extends RaftState {
        private Context cxt;

        Leader() {
            super(State.LEADER);
        }

        public void start(Context cxt) {
            logger.debug("node {} start leader", RaftImpl.this);
            this.cxt = cxt;
            RaftImpl.this.broadcastPing();
            stateMachine.onLeaderStart(getStatus());
        }

        public Context finish() {
            logger.debug("node {} finish leader", RaftImpl.this);
            stateMachine.onLeaderFinish(getStatus());
            pendingProposal.failedAllFutures();
            if (transferLeaderFuture != null) {
                transferLeaderFuture.getResponseFuture().complete(ProposalResponse.success());
                transferLeaderFuture = null;
            }
            return cxt;
        }

        @Override
        public void onElectionTimeout() {
            if (transferLeaderFuture != null) {
                logger.info("node {} abort transfer leadership to {} due to timeout", RaftImpl.this,
                        transferLeaderFuture.getTransfereeId());
                transferLeaderFuture.getResponseFuture()
                        .complete(ProposalResponse.error(ErrorMsg.TIMEOUT));
                transferLeaderFuture = null;
            }
        }

        @Override
        public void onPingTimeout() {
            RaftImpl.this.broadcastPing();
        }

        @Override
        void process(RaftCommand cmd) {
            final long selfTerm = RaftImpl.this.meta.getTerm();
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

                            if (transferLeaderFuture != null
                                    && transferLeaderFuture.getTransfereeId().equals(cmd.getFrom())
                                    && node.getMatchIndex() == raftLog.getLastIndex()) {
                                logger.info("node {} send timeout to {} after it has up to date logs", RaftImpl.this, cmd.getFrom());
                                node.sendTimeout(selfTerm);
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
        private Context cxt;

        Follower() {
            super(State.FOLLOWER);
        }

        public void start(Context cxt) {
            logger.debug("node {} start follower", RaftImpl.this);
            this.cxt = cxt;
            stateMachine.onFollowerStart(getStatus());
        }

        public Context finish() {
            logger.debug("node {} finish follower", RaftImpl.this);
            stateMachine.onFollowerFinish(getStatus());
            return cxt;
        }

        @Override
        public void onElectionTimeout() {
            logger.info("election timeout, node {} become candidate", RaftImpl.this);

            RaftImpl.this.tryBecomeCandidate();
        }

        @Override
        void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES:
                    RaftImpl.this.clearTickCounters();
                    RaftImpl.this.leaderId = cmd.getLeaderId();
                    RaftImpl.this.processAppendEntries(cmd);
                    break;
                case PING:
                    RaftImpl.this.clearTickCounters();
                    RaftImpl.this.leaderId = cmd.getFrom();
                    RaftImpl.this.processHeartbeat(cmd);
                    break;
                case SNAPSHOT:
                    RaftImpl.this.clearTickCounters();
                    RaftImpl.this.leaderId = cmd.getFrom();
                    RaftImpl.this.processSnapshot(cmd);
                    break;
                case TIMEOUT_NOW:
                    if (peerNodes.containsKey(selfId)) {
                        cxt.receiveTimeoutOnTransferLeader = true;
                        RaftImpl.this.tryBecomeCandidate();
                    } else {
                        logger.info("node {} receive timeout but it was removed from cluster already. currentPeers: {}",
                                RaftImpl.this, peerNodes.keySet());
                    }
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }
    }

    private class Candidate extends RaftState {
        private Context cxt;
        private volatile ConcurrentHashMap<String, Boolean> votesGot = new ConcurrentHashMap<>();

        Candidate() {
            super(State.CANDIDATE);
        }

        public void start(Context cxt) {
            logger.debug("node {} start candidate", RaftImpl.this);
            this.cxt = cxt;
            startElection();
            stateMachine.onCandidateStart(getStatus());
        }

        public Context finish() {
            logger.debug("node {} finish candidate", RaftImpl.this);
            stateMachine.onCandidateFinish(getStatus());
            return cxt;
        }

        private void startElection() {
            boolean forceElection = cxt.receiveTimeoutOnTransferLeader;
            cxt.receiveTimeoutOnTransferLeader = false;

            votesGot = new ConcurrentHashMap<>();
            RaftImpl.this.meta.setVotedFor(RaftImpl.this.selfId);
            assert RaftImpl.this.getState() == State.CANDIDATE;

            long oldTerm = RaftImpl.this.meta.getTerm();
            final long candidateTerm = oldTerm + 1;
            RaftImpl.this.meta.setTerm(candidateTerm);

            // got self vote initially
            final int votesToWin = RaftImpl.this.getQuorum() - 1;

            if (votesToWin == 0) {
                RaftImpl.this.tryBecomeLeader();
            } else {
                long lastIndex = raftLog.getLastIndex();
                Optional<Long> term = raftLog.getTerm(lastIndex);

                assert term.isPresent();

                logger.debug("node {} start election, votesToWin={}, lastIndex={}, lastTerm={}",
                        RaftImpl.this, votesToWin, lastIndex, term.get());
                for (final RaftPeerNode node : RaftImpl.this.getPeerNodes().values()) {
                    if (!node.getPeerId().equals(RaftImpl.this.selfId)) {
                        RaftCommand vote = RaftCommand.newBuilder()
                                .setType(RaftCommand.CmdType.REQUEST_VOTE)
                                .setTerm(candidateTerm)
                                .setFrom(RaftImpl.this.selfId)
                                .setLastLogIndex(lastIndex)
                                .setLastLogTerm(term.get())
                                .setTo(node.getPeerId())
                                .setForceElection(forceElection)
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
                case SNAPSHOT:
                    RaftImpl.this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
                    RaftImpl.this.processSnapshot(cmd);
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }

        @Override
        public void onElectionTimeout() {
            RaftImpl.this.tryBecomeCandidate();
        }
    }
}
