package raft.server;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;
import raft.server.log.RaftLogImpl;
import raft.server.proto.ConfigChange;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;
import raft.server.proto.RaftCommand;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.stream.Collectors;

import static raft.server.util.Preconditions.checkArgument;
import static raft.server.util.Preconditions.checkNotNull;
import static raft.server.util.Preconditions.checkState;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftImpl implements Raft {
    private static final Logger logger = LoggerFactory.getLogger(RaftImpl.class.getSimpleName());
    private static final AtomicReferenceFieldUpdater<RaftImpl, State> STATE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(RaftImpl.class, State.class, "state");

    private final RaftCommandProcessor leader = new Leader();
    private final RaftCommandProcessor candidate = new Candidate();
    private final RaftCommandProcessor follower = new Follower();

    private final Map<String, RaftPeerNode> peerNodes;
    private final BlockingQueue<RaftJob> jobQueue;
    private final PendingProposalFutures pendingProposal;
    private final RaftConfigurations config;
    private final Thread workerThread;
    private final String selfId;
    private final RaftLog raftLog;
    private final LocalFilePersistentMeta meta;
    private final StateMachineProxy stateMachine;
    private final RaftCommandBroker broker;
    private final TimeoutManager timeoutManager;

    private volatile String leaderId;
    private volatile State state;
    private volatile boolean workerRun;
    private volatile CompletableFuture<Void> shutdownFuture;

    private RaftCommandProcessor cmdProcessor;
    private boolean existsPendingConfigChange;
    private TransferLeaderFuture transferLeaderFuture;
    private boolean autoFlush;
    private boolean forceElection;

    public RaftImpl(RaftConfigurations c) {
        checkNotNull(c);

        this.config = c;
        // TODO configurable job queue size
        this.jobQueue = new LinkedBlockingQueue<>(1000);
        this.pendingProposal = new PendingProposalFutures();
        this.workerRun = true;
        this.workerThread = new Thread(new Worker());
        this.raftLog = new RaftLogImpl(c.storage);

        this.timeoutManager = new DefaultTimeoutManager(config, (electionTimeout, pingTimeout) ->
                jobQueue.add(new TimeoutJob(electionTimeout, pingTimeout))
        );

        this.selfId = c.selfId;
        this.meta = new LocalFilePersistentMeta(c.persistentMetaFileDirPath, c.selfId, c.syncWriteStateFile);
        this.stateMachine = new StateMachineProxy(c.stateMachine, this.raftLog);
        this.autoFlush = true;
        this.broker = c.broker;

        this.peerNodes = new HashMap<>();
        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1, c.maxEntriesPerAppend));
        }

        this.state = State.UNINITIALIZED;
        this.cmdProcessor = follower;
    }

    @Override
    public Raft start() {
        if (!STATE_UPDATER.compareAndSet(this, State.UNINITIALIZED, State.INITIALIZING)) {
            return this;
        }

        meta.init();
        raftLog.init(meta);
        if (config.appliedTo > -1) {
            raftLog.appliedTo(config.appliedTo);
        }

        reset(meta.getTerm());
        timeoutManager.start();

        workerThread.start();

        logger.info("node {} started with:\n" +
                        "term={}\n" +
                        "votedFor={}\n" +
                        "electionTimeout={}\n" +
                        "tickIntervalMs={}\n" +
                        "pingIntervalTicks={}\n" +
                        "suggectElectionTimeoutTicks={}\n" +
                        "raftLog={}\n",
                this, meta.getTerm(), meta.getVotedFor(), timeoutManager.getElectionTimeoutTicks(),
                config.tickIntervalMs, config.pingIntervalTicks,
                config.suggestElectionTimeoutTicks, raftLog);
        return this;
    }

    private final class TimeoutJob implements RaftJob {
        private final boolean electionTimeout;
        private final boolean pingTimeout;

        TimeoutJob(boolean electionTimeout, boolean pingTimeout) {
            this.electionTimeout = electionTimeout;
            this.pingTimeout = pingTimeout;
        }

        @Override
        public void processJob() {
            try {
                if (state.isActive()) {
                    if (electionTimeout) {
                        cmdProcessor.onElectionTimeout();
                    }

                    if (pingTimeout) {
                        cmdProcessor.onPingTimeout();
                    }
                }
            } finally {
                timeoutManager.clearAllTimeoutMark();
            }
        }
    }

    @Override
    public String getId() {
        return selfId;
    }

    private State getState() {
        return state;
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

    private Map<String, RaftPeerNode> getPeerNodes() {
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
        checkState(state.isActive(), "raft server not start or already shutdown");

        return proposeConfigChange(newNode, ConfigChange.ConfigChangeAction.ADD_NODE);
    }

    @Override
    public CompletableFuture<ProposalResponse> removeNode(String newNode) {
        checkNotNull(newNode);
        checkArgument(!newNode.isEmpty());
        checkState(state.isActive(), "raft server not start or already shutdown");

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
        checkState(state.isActive(), "raft server not start or already shutdown");

        ArrayList<byte[]> data = new ArrayList<>();
        data.add(transfereeId.getBytes(StandardCharsets.UTF_8));

        return doPropose(data, LogEntry.EntryType.TRANSFER_LEADER);
    }

    @Override
    public CompletableFuture<ProposalResponse> propose(List<byte[]> datas) {
        checkNotNull(datas);
        checkArgument(!datas.isEmpty());
        checkState(state.isActive(), "raft server not start or already shutdown");

        logger.debug("node {} receives proposal with {} entries", this, datas.size());

        return doPropose(datas, LogEntry.EntryType.LOG);
    }

    private CompletableFuture<ProposalResponse> doPropose(final List<byte[]> datas, final LogEntry.EntryType type) {
        Proposal proposal = new Proposal(datas, type);
        CompletableFuture<ProposalResponse> future;
        if (getState() == State.LEADER) {
            future = proposal.getFuture();
            if (!jobQueue.add(new ProposalJob(proposal))) {
                future = CompletableFuture.completedFuture(
                        ProposalResponse.errorWithLeaderHint(leaderId, ErrorMsg.TOO_MANY_PENDING_PROPOSALS));
            }
        } else {
            future = CompletableFuture.completedFuture(ProposalResponse.errorWithLeaderHint(leaderId, ErrorMsg.NOT_LEADER));
        }
        return future;
    }

    void writeOutCommand(RaftCommand.Builder builder) {
        builder.setFrom(selfId);
        if (autoFlush) {
            broker.onWriteAndFlushCommand(builder.build());
        } else {
            broker.onWriteCommand(builder.build());
        }
    }

    @Override
    public void receiveCommand(RaftCommand cmd) {
        checkNotNull(cmd);
        checkState(state.isActive(), "raft server not start or already shutdown");

        jobQueue.add(new RaftCommandJob(cmd));
    }

    private final class RaftCommandJob implements RaftJob {
        private RaftCommand cmd;

        private RaftCommandJob(RaftCommand cmd) {
            this.cmd = cmd;
        }

        @Override
        public void processJob() {
            try {
                if (!peerNodes.containsKey(cmd.getFrom())) {
                    logger.warn("node {} received cmd {} from unknown peer", this, cmd);
                    return;
                }

                processReceivedCommand(cmd);
            } catch (Throwable t) {
                logger.error("process received command {} on node {} failed", cmd, this, t);
                //TODO panic when process received command failed?
            }
        }
    }

    private class Worker implements Runnable {
        @Override
        public void run() {
            // init state
            state = cmdProcessor.getBindingState();
            cmdProcessor.start();

            while (workerRun) {
                try {
                    RaftJob j = jobQueue.take();
                    j.processJob();
                } catch (InterruptedException e) {
                    // ignore maybe we are shutting down
                } catch (Throwable t) {
                    panic("unexpected exception", t);
                }
            }
        }
    }

    private void processReceivedCommand(RaftCommand cmd) {
        final long selfTerm = meta.getTerm();
        if (cmd.getType() == RaftCommand.CmdType.REQUEST_VOTE) {
            if (!cmd.getForceElection() && leaderId != null && timeoutManager.isElectionTimeout()) {
                // if a server receives a RequestVote request within the minimum election timeout of hearing
                // from a current leader, it does not update its term or grant its vote except the force election mark
                // is set which indicate that this REQUEST_VOTE command is from a legitimate server during leader
                // transfer operation
                logger.info("node {} ignore request vote cmd: {} because it's leader still valid.}",
                        this, cmd);
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

            cmdProcessor.process(cmd);
        }
    }

    private final class ProposalJob implements RaftJob {
        private Proposal proposal;

        private ProposalJob(Proposal proposal) {
            this.proposal = proposal;
        }

        @Override
        public void processJob() {
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
                                leaderAppendEntries(proposal.getDatas(), proposal.getType(), proposal.getFuture());
                                return;
                            case TRANSFER_LEADER:
                                String transfereeId = new String(proposal.getDatas().get(0).toByteArray(), StandardCharsets.UTF_8);

                                if (transfereeId.equals(selfId)) {
                                    error = ErrorMsg.ALLREADY_LEADER;
                                    break;
                                }

                                if (!peerNodes.containsKey(transfereeId)) {
                                    error = ErrorMsg.UNKNOWN_TRANSFEREEID;
                                    break;
                                }

                                logger.info("node {} start transfer leadership to {}", this, transfereeId);

                                timeoutManager.clearElectionTickCounter();
                                transferLeaderFuture = new TransferLeaderFuture(transfereeId, proposal.getFuture());
                                RaftPeerNode n = peerNodes.get(transfereeId);
                                if (n.getMatchIndex() == raftLog.getLastIndex()) {
                                    logger.info("node {} send timeout immediately to {} because it already " +
                                            "has up to date logs", this, transfereeId);
                                    n.sendTimeout(meta.getTerm());
                                } else {
                                    n.sendAppend(meta.getTerm(), true);
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
    }

    private void leaderAppendEntries(List<ByteString> datas, LogEntry.EntryType type, CompletableFuture<ProposalResponse> responseFuture) {
        long term = meta.getTerm();

        final ArrayList<LogEntry> preparedEntries = new ArrayList<>(datas.size());
        long newLastIndex = raftLog.getLastIndex();
        for (ByteString data : datas) {
            ++newLastIndex;
            LogEntry e = LogEntry.newBuilder()
                    .setIndex(newLastIndex)
                    .setTerm(term)
                    .setType(type)
                    .setData(data)
                    .build();
            preparedEntries.add(e);
        }

        raftLog.leaderAsyncAppend(preparedEntries).whenComplete((lastIndex, err) -> {
            if (err != null) {
                panic("async append logs failed", err);
            } else {
                jobQueue.add(new LeaderUpdateCommitJob(lastIndex));
            }
        });

        pendingProposal.addFuture(newLastIndex, responseFuture);
        broadcastAppendEntries();
    }

    private class LeaderUpdateCommitJob implements RaftJob {
        private final long lastIndex;

        private LeaderUpdateCommitJob(long lastIndex) {
            this.lastIndex = lastIndex;
        }

        @Override
        public void processJob() {
            logger.debug("node {} try update index to {}", this, lastIndex);
            // it's OK if this node has stepped-down and surrendered leadership before we
            // updated index because we don't use RaftPeerNode on follower or candidate
            RaftPeerNode leaderNode = peerNodes.get(selfId);
            if (leaderNode != null) {
                if (leaderNode.updateIndexes(lastIndex)) {
                    updateCommit();
                }
            } else {
                logger.error("node {} was removed from remote peer set {}",
                        this, peerNodes.keySet());
            }

            logger.debug("node {} try update index to {}", this, lastIndex);
        }
    }

    private void broadcastAppendEntries() {
        final long selfTerm = meta.getTerm();
        for (final RaftPeerNode peer : peerNodes.values()) {
            if (!peer.getPeerId().equals(selfId)) {
                peer.sendAppend(selfTerm, true);
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
                        config.maxEntriesPerAppend));

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
                    return tryCommitTo(kthMatchedIndexes);
                }
            }
        }
        return false;
    }

    private void broadcastPing() {
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
            // reinitialize nextIndex for every peer node
            long lastIndex = raftLog.getLastIndex();
            for (RaftPeerNode node : peerNodes.values()) {
                node.reset(lastIndex + 1);
                if (node.getPeerId().equals(selfId)) {
                    node.setMatchIndex(lastIndex);
                }
            }

            transitState(leader, meta.getTerm(), selfId);
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

        timeoutManager.clearAllTickCounters();
        // we need to reset election timeout on every time state changed and every
        // reelection in candidate state to avoid split vote
        timeoutManager.resetElectionTimeoutTicks();
        timeoutManager.clearAllTimeoutMark();
    }

    private void transitState(RaftCommandProcessor nextState, long newTerm, String newLeaderId) {
        assert Thread.currentThread() == workerThread;

        // we'd better finish old state before reset term and set leader id. this can insure that old state
        // finished with the old term and leader id while new state started with new term and new leader id
        cmdProcessor.finish();
        cmdProcessor = nextState;
        state = nextState.getBindingState();
        reset(newTerm);
        if (newLeaderId != null) {
            this.leaderId = newLeaderId;
        }
        nextState.start();
    }

    private void processAppendEntries(RaftCommand cmd) {
        final RaftCommand.Builder resp = RaftCommand.newBuilder()
                .setType(RaftCommand.CmdType.APPEND_ENTRIES_RESP)
                .setTo(cmd.getFrom())
                .setTerm(meta.getTerm())
                .setSuccess(false);
        State state = getState();

        if (cmd.getPrevLogIndex() < raftLog.getCommitIndex()) {
            resp.setMatchIndex(raftLog.getCommitIndex()).setSuccess(true);
            writeOutCommand(resp);
        } else if (state != State.FOLLOWER) {
            logger.debug("append logs failed on node {} due to invalid state: {}", this, state);
            resp.setLastLogIndex(raftLog.getLastIndex());
            writeOutCommand(resp);
        } else {
            replicateLogsOnFollower(cmd);
        }
    }

    private final class ReplicateLogsFinishedJob implements RaftJob {
        private final RaftCommand receivedCmd;
        private final long matchIndex;

        private ReplicateLogsFinishedJob(RaftCommand receivedReplicateLogsCmd, long matchIndex) {
            this.receivedCmd = receivedReplicateLogsCmd;
            this.matchIndex = matchIndex;
        }

        @Override
        public void processJob() {
            autoFlush = true;
            broker.onFlushCommand();

            RaftCommand.Builder resp = RaftCommand.newBuilder()
                    .setType(RaftCommand.CmdType.APPEND_ENTRIES_RESP)
                    .setTo(receivedCmd.getFrom())
                    .setTerm(meta.getTerm())
                    .setSuccess(false);
            if (matchIndex != -1L) {
                tryCommitTo(Math.min(receivedCmd.getLeaderCommit(), matchIndex));

                resp.setSuccess(true);
                resp.setMatchIndex(matchIndex);
            } else {
                resp.setLastLogIndex(raftLog.getLastIndex());
            }
            writeOutCommand(resp);
        }
    }

    private void replicateLogsOnFollower(RaftCommand cmd) {
        // do not flush any command out before logs written
        autoFlush = false;

        long prevIndex = cmd.getPrevLogIndex();
        long prevTerm = cmd.getPrevLogTerm();
        String leaderId = cmd.getLeaderId();
        List<raft.server.proto.LogEntry> entries = cmd.getEntriesList();

        this.leaderId = leaderId;
        raftLog.followerAsyncAppend(prevIndex, prevTerm, entries)
                .whenComplete((matchIndex, ex) -> {
                    if (ex != null) {
                        // TODO halt this node?
                        logger.error("append entries failed on node {}, leaderId {}, entries {}",
                                this, leaderId, entries, ex);
                        matchIndex = -1L;
                    }
                    jobQueue.add(new ReplicateLogsFinishedJob(cmd, matchIndex));
                });

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

    private boolean tryCommitTo(long commitTo) {
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
            return true;
        }
        return false;
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
                    config.maxEntriesPerAppend);
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

    private final class ShutdownJob implements RaftJob {

        @Override
        public void processJob() {
            try {
                assert state == State.SHUTTING_DOWN;

                logger.info("shutting down node {} ...", this);
                timeoutManager.shutdown();

                raftLog.awaitTermination();
                stateMachine.onShutdown();
                broker.shutdown();
                workerRun = false;
            } catch (InterruptedException ex) {
                logger.warn("node {} shutdown process interrupted", this);
                shutdownFuture.completeExceptionally(ex);
            } catch (Throwable ex) {
                logger.info("node {} shutdown failed", this);
                shutdownFuture.completeExceptionally(ex);
            } finally {
                logger.info("node {} shutdown", this);
                state = State.SHUTDOWN;
                shutdownFuture.complete(null);
            }
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        for (; ; ) {
            State state = this.state;
            if (state.isShutingDown()) {
                assert shutdownFuture != null;

                return shutdownFuture;
            }

            if (STATE_UPDATER.compareAndSet(this, state, State.SHUTTING_DOWN)) {
                shutdownFuture = new CompletableFuture<>();
                jobQueue.add(new ShutdownJob());
                return shutdownFuture;
            }
        }
    }

    @Override
    public void awaitTermination() throws InterruptedException, ExecutionException {
        for (; ; ) {
            if (shutdownFuture == null) {
                shutdown();
            } else {
                shutdownFuture.get();
                return;
            }
        }
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

    private class Leader implements RaftCommandProcessor {
        public void start() {
            leaderAppendEntries(Collections.singletonList(ByteString.EMPTY), LogEntry.EntryType.LOG, Proposal.voidFuture);
            stateMachine.onLeaderStart(getStatus());
        }

        @Override
        public State getBindingState() {
            return State.LEADER;
        }

        public void finish() {
            logger.debug("node {} finish leader", RaftImpl.this);
            stateMachine.onLeaderFinish(getStatus());
            pendingProposal.failedAllFutures();
            if (transferLeaderFuture != null) {
                transferLeaderFuture.getResponseFuture().complete(ProposalResponse.success());
                transferLeaderFuture = null;
            }
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
        public void process(RaftCommand cmd) {
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
                            if (node.updateIndexes(cmd.getMatchIndex())) {
                                node.onReceiveAppendSuccess(cmd.getMatchIndex());

                                if (updateCommit()) {
                                    broadcastAppendEntries();
                                }

                                while (true) {
                                    if (!node.sendAppend(selfTerm, false)) {
                                        break;
                                    }
                                }

                                if (transferLeaderFuture != null
                                        && transferLeaderFuture.getTransfereeId().equals(cmd.getFrom())
                                        && node.getMatchIndex() == raftLog.getLastIndex()) {
                                    logger.info("node {} send timeout to {} after it has up to date logs", RaftImpl.this, cmd.getFrom());
                                    node.sendTimeout(selfTerm);
                                }
                            }
                        } else {
                            node.decreaseIndexAndResendAppend(selfTerm);
                        }
                    }
                    break;
                case PONG:
                    // resend pending append entries
                    RaftPeerNode node = peerNodes.get(cmd.getFrom());
                    node.onPongRecieved();
                    if (node.getMatchIndex() < RaftImpl.this.raftLog.getLastIndex()) {
                        node.sendAppend(selfTerm, true);
                    }
                    break;
                case REQUEST_VOTE_RESP:
                    break;
                default:
                    logger.warn("node {} received unexpected command {}", RaftImpl.this, cmd);
            }
        }
    }

    private class Follower implements RaftCommandProcessor {
        public void start() {
            logger.debug("node {} start follower", RaftImpl.this);
            stateMachine.onFollowerStart(getStatus());
        }

        @Override
        public State getBindingState() {
            return State.FOLLOWER;
        }

        public void finish() {
            logger.debug("node {} finish follower", RaftImpl.this);
            stateMachine.onFollowerFinish(getStatus());
        }

        @Override
        public void onElectionTimeout() {
            logger.info("election timeout, node {} become candidate", RaftImpl.this);

            RaftImpl.this.tryBecomeCandidate();
        }

        @Override
        public void process(RaftCommand cmd) {
            switch (cmd.getType()) {
                case APPEND_ENTRIES:
                    timeoutManager.clearAllTickCounters();
                    leaderId = cmd.getLeaderId();
                    processAppendEntries(cmd);
                    break;
                case PING:
                    timeoutManager.clearAllTickCounters();
                    leaderId = cmd.getFrom();
                    processHeartbeat(cmd);
                    break;
                case SNAPSHOT:
                    timeoutManager.clearAllTickCounters();
                    leaderId = cmd.getFrom();
                    processSnapshot(cmd);
                    break;
                case TIMEOUT_NOW:
                    if (peerNodes.containsKey(selfId)) {
                        forceElection = true;
                        tryBecomeCandidate();
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

    private class Candidate implements RaftCommandProcessor {
        private volatile ConcurrentHashMap<String, Boolean> votesGot = new ConcurrentHashMap<>();

        public void start() {
            logger.debug("node {} start candidate", RaftImpl.this);
            startElection();
            stateMachine.onCandidateStart(getStatus());
        }

        @Override
        public State getBindingState() {
            return State.CANDIDATE;
        }

        public void finish() {
            logger.debug("node {} finish candidate", RaftImpl.this);
            stateMachine.onCandidateFinish(getStatus());
        }

        private void startElection() {
            boolean forceElection = RaftImpl.this.forceElection;
            RaftImpl.this.forceElection = false;

            votesGot = new ConcurrentHashMap<>();
            meta.setVotedFor(selfId);
            assert RaftImpl.this.getState() == State.CANDIDATE;

            long oldTerm = meta.getTerm();
            final long candidateTerm = oldTerm + 1;
            meta.setTerm(candidateTerm);

            // got self vote initially
            final int votesToWin = RaftImpl.this.getQuorum() - 1;

            if (votesToWin == 0) {
                tryBecomeLeader();
            } else {
                long lastIndex = raftLog.getLastIndex();
                Optional<Long> term = raftLog.getTerm(lastIndex);

                assert term.isPresent();

                logger.debug("node {} start election, votesToWin={}, lastIndex={}, lastTerm={}",
                        RaftImpl.this, votesToWin, lastIndex, term.get());
                for (final RaftPeerNode node : getPeerNodes().values()) {
                    if (!node.getPeerId().equals(selfId)) {
                        RaftCommand.Builder vote = RaftCommand.newBuilder()
                                .setType(RaftCommand.CmdType.REQUEST_VOTE)
                                .setTerm(candidateTerm)
                                .setFrom(selfId)
                                .setLastLogIndex(lastIndex)
                                .setLastLogTerm(term.get())
                                .setTo(node.getPeerId())
                                .setForceElection(forceElection);
                        writeOutCommand(vote);
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
