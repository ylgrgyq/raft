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
import raft.server.proto.RaftCommand;
import raft.server.proto.Snapshot;

import java.util.*;
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
    private final AtomicLong electionTickCounter = new AtomicLong();
    private final AtomicBoolean electionTickerTimeout = new AtomicBoolean();
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
    private final RaftPersistentState meta;
    private final StateMachineProxy stateMachine;
    private final AsyncRaftCommandBrokerProxy broker;

    private String leaderId;
    private RaftState state;
    private long electionTimeoutTicks;
    private Thread workerThread;
    private volatile boolean workerRun = true;
    private boolean existsPendingConfigChange = false;
    private TransferLeaderFuture transferLeaderFuture = null;

    RaftImpl(Config c) {
        Preconditions.checkNotNull(c);

        this.c = c;
        this.workerThread = new Thread(this);
        this.raftLog = new RaftLogImpl(c.storage);
        this.tickGenerator = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("tick-generator-"));

        this.selfId = c.selfId;
        this.meta = new RaftPersistentState(c.persistentStateFileDirPath, c.selfId);
        this.stateMachine = new StateMachineProxy(c.stateMachine, this.raftLog);
        this.broker = new AsyncRaftCommandBrokerProxy(c.broker);

        for (String peerId : c.peers) {
            this.peerNodes.put(peerId, new RaftPeerNode(peerId, this, this.raftLog, 1, c.maxEntriesPerAppend));
        }

        this.state = follower;
    }

    void start() {
        meta.init();
        reset(meta.getTerm());

        raftLog.init(meta);
        if (c.appliedTo > -1) {
            raftLog.appliedTo(c.appliedTo);
        }

        tickGenerator.scheduleWithFixedDelay(() -> {
            boolean wakeup = false;
            if (electionTickCounter.incrementAndGet() >= electionTimeoutTicks) {
                electionTickCounter.set(0);
                electionTickerTimeout.compareAndSet(false, true);
                wakeup = true;
            }

            if (pingTickCounter.incrementAndGet() >= c.pingIntervalTicks) {
                pingTickCounter.set(0);
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
    }

    boolean isLeader() {
        return getState() == State.LEADER;
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return peerNodes;
    }

    private int getQuorum() {
        return peerNodes.size() / 2 + 1;
    }

    State getState() {
        return state.getState();
    }

    String getLeaderId() {
        return leaderId;
    }

    String getSelfId() {
        return selfId;
    }

    Optional<Snapshot> getRecentSnapshot(int expectIndex) {
        return stateMachine.getRecentSnapshot(expectIndex);
    }

    // FIXME state may change during getting status
    RaftStatus getStatus() {
        RaftStatus status = new RaftStatus();
        status.setTerm(meta.getTerm());
        status.setCommitIndex(raftLog.getCommitIndex());
        status.setAppliedIndex(raftLog.getAppliedIndex());
        status.setId(selfId);
        status.setLeaderId(leaderId);
        status.setVotedFor(meta.getVotedFor());
        status.setState(getState());
        status.setPeerNodeIds(new ArrayList<>(peerNodes.keySet()));

        return status;
    }

    CompletableFuture<ProposalResponse> proposeData(final List<byte[]> entries) {
        logger.debug("node {} receives proposal with {} entries", this, entries.size());

        return doPropose(entries, LogEntry.EntryType.LOG);
    }

    CompletableFuture<ProposalResponse> proposeConfigChange(final String peerId, final ConfigChange.ConfigChangeAction action) {
        ConfigChange change = ConfigChange.newBuilder()
                .setAction(action)
                .setPeerId(peerId)
                .build();

        ArrayList<byte[]> data = new ArrayList<>();
        data.add(change.toByteArray());

        return doPropose(data, LogEntry.EntryType.CONFIG);
    }

    CompletableFuture<ProposalResponse> proposeTransferLeader(final String transfereeId) {
        ArrayList<byte[]> data = new ArrayList<>();
        data.add(transfereeId.getBytes());

        return doPropose(data, LogEntry.EntryType.TRANSFER_LEADER);
    }

    private CompletableFuture<ProposalResponse> doPropose(final List<byte[]> datas, final LogEntry.EntryType type) {
        List<LogEntry> logEntries =
                datas.stream().map(d ->
                        LogEntry.newBuilder()
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
        RaftImpl.this.state.start(new Context());

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
        if (electionTickerTimeout.compareAndSet(true, false)) {
            state.onElectionTimeout();
        }

        if (pingTickerTimeout.compareAndSet(true, false)) {
            state.onPingTimeout();
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
        final int selfTerm = meta.getTerm();
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
                            final int term = meta.getTerm();
                            int newLastIndex = raftLog.leaderAsyncAppend(term, proposal.getEntries(), (lastIndex, err) -> {
                                if (err != null) {
                                    panic("async append logs failed", err);
                                } else {
                                    // it's OK if this node has stepped-down and surrendered leadership before we
                                    // update index because we don't use RaftPeerNode on follower or candidate
                                    RaftPeerNode leaderNode = peerNodes.get(selfId);
                                    leaderNode.updateIndexes(lastIndex);
                                }
                            });

                            pendingProposal.addFuture(newLastIndex, proposal.getFuture());
                            broadcastAppendEntries();
                            return;
                        case TRANSFER_LEADER:
                            String transereeId = new String(proposal.getEntries().get(0).getData().toByteArray());

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
            final int selfTerm = meta.getTerm();
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (!peer.getPeerId().equals(selfId)) {
                    peer.sendAppend(selfTerm);
                }
            }
        }
    }

    private void processNewCommitedLogs(List<LogEntry> commitedLogs) {
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
            } else {
                withoutConfigLogs.add(e);
            }
        }

        LogEntry lastLog = commitedLogs.get(commitedLogs.size() - 1);
        stateMachine.onProposalCommitted(withoutConfigLogs, lastLog.getIndex());
    }

    private void addNode(final String peerId) {
        peerNodes.computeIfAbsent(peerId,
                k -> new RaftPeerNode(peerId,
                        this,
                        raftLog,
                        raftLog.getLastIndex() + 1,
                        c.maxEntriesPerAppend));

        logger.info("node {} addFuture peerId \"{}\" to cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
        stateMachine.onNodeAdded(peerId);

        existsPendingConfigChange = false;
    }

    private void removeNode(final String peerId) {
        peerNodes.remove(peerId);

        logger.info("node {} remove peerId \"{}\" from cluster. currentPeers: {}", this, peerId, peerNodes.keySet());
        stateMachine.onNodeRemoved(peerId);

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
        List<Integer> matchedIndexes = peerNodes.values().stream()
                .map(RaftPeerNode::getMatchIndex).sorted().collect(Collectors.toList());
        int kthMatchedIndexes = matchedIndexes.get(k);
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
        final int selfTerm = meta.getTerm();
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

    private boolean tryBecomeFollower(int term, String leaderId) {
        assert Thread.currentThread() == workerThread;

        final int selfTerm = meta.getTerm();
        if (term >= selfTerm) {
            transitState(follower, term, leaderId);
            return true;
        } else {
            logger.error("node {} transient state to {} failed, term = {}, leaderId = {}",
                    this, State.FOLLOWER, term, leaderId);
            return false;
        }
    }

    private boolean tryBecomeLeader() {
        assert Thread.currentThread() == workerThread;

        if (getState() == State.CANDIDATE) {
            transitState(leader, meta.getTerm(), selfId);

            // reinitialize nextIndex for every peer node
            // then send them initial ping on start leader state
            for (RaftPeerNode node : peerNodes.values()) {
                node.reset(raftLog.getLastIndex() + 1);
            }
            return true;
        } else {
            logger.error("node {} transient state to {} failed", this, State.LEADER);
            return false;
        }
    }

    private boolean tryBecomeCandidate() {
        assert Thread.currentThread() == workerThread;

        transitState(candidate, meta.getTerm(), null);

        return true;
    }

    private void reset(int term) {
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

    private void transitState(RaftState nextState, int newTerm, String newLeaderId) {
        assert Thread.currentThread() == workerThread;

        // we'd better finish old state before reset term and set leader id. this can insure that when old state
        // finished with the old term and leader id while new state started with new term and leader id
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
            int matchIndex = RaftImpl.this.replicateLogsOnFollower(cmd);
            if (matchIndex != -1) {
                tryCommitTo(Math.min(cmd.getLeaderCommit(), matchIndex));

                resp.setSuccess(true);
                resp.setMatchIndex(matchIndex);
            }
        }
        writeOutCommand(resp);
    }

    private int replicateLogsOnFollower(RaftCommand cmd) {
        int prevIndex = cmd.getPrevLogIndex();
        int prevTerm = cmd.getPrevLogTerm();
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

    private void tryCommitTo(int commitTo) {
        if (logger.isDebugEnabled()) {
            logger.debug("node {} try commit to {} with current commitIndex: {} and lastIndex: {}",
                    this, commitTo, raftLog.getCommitIndex(), raftLog.getLastIndex());
        }

        List<LogEntry> newCommitedLogs = raftLog.tryCommitTo(commitTo);
        if (!newCommitedLogs.isEmpty()) {
            meta.setCommitIndex(commitTo);
            processNewCommitedLogs(newCommitedLogs);
            pendingProposal.completeFutures(commitTo);
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

    private boolean tryApplySnapshot(Snapshot snapshot) {
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
        stateMachine.installSnapshot(snapshot);

        int lastIndex = raftLog.getLastIndex();
        Set<String> remainPeerIds = new HashSet<>();
        remainPeerIds.addAll(peerNodes.keySet());

        for (String peerId : snapshot.getPeerIdsList()) {
            RaftPeerNode node = new RaftPeerNode(peerId, this, raftLog, lastIndex + 1,
                    c.maxEntriesPerAppend);
            if (peerId.equals(selfId)) {
                node.updateIndexes(lastIndex - 1);
            }
            peerNodes.put(peerId, node);
            remainPeerIds.remove(peerId);
        }

        for (String id : remainPeerIds) {
            peerNodes.remove(id);
        }

        return true;
    }

    private void panic(String reason, Throwable err) {
        logger.error("node {} panic due to {}, shutdown immediately", this, reason, err);
        shutdown();
    }

    void shutdown() {
        logger.info("shutting down node {} ...", this);
        tickGenerator.shutdown();
        workerRun = false;
        wakeUpWorker();
        try {
            workerThread.join();
        } catch (InterruptedException ex) {
            // ignore and continue shutdown
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
            final int selfTerm = RaftImpl.this.meta.getTerm();
            stateMachine.onLeaderStart(selfTerm);
        }

        public Context finish() {
            logger.debug("node {} finish leader", RaftImpl.this);
            stateMachine.onLeaderFinish();
            if (transferLeaderFuture != null) {
                transferLeaderFuture.getResponseFuture().complete(ProposalResponse.success());
                transferLeaderFuture = null;
            }
            return cxt;
        }

        @Override
        public void onElectionTimeout() {
            if (transferLeaderFuture  != null) {
                logger.info("node {} abort transfer leadership to {} due to timeout", RaftImpl.this,
                        transferLeaderFuture.getTransfereeId());
                transferLeaderFuture.getResponseFuture()
                        .complete(ProposalResponse.error(ErrorMsg.TIMEOUT));
                transferLeaderFuture  = null;
            }
        }

        @Override
        public void onPingTimeout() {
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
            final int selfTerm = RaftImpl.this.meta.getTerm();
            stateMachine.onFollowerStart(selfTerm, RaftImpl.this.getLeaderId());
        }

        public Context finish() {
            logger.debug("node {} finish follower", RaftImpl.this);
            stateMachine.onFollowerFinish();
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
        }

        public Context finish() {
            logger.debug("node {} finish candidate", RaftImpl.this);
            return cxt;
        }

        private void startElection() {
            boolean forceElection = cxt.receiveTimeoutOnTransferLeader;
            cxt.receiveTimeoutOnTransferLeader = false;

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
                int lastIndex = raftLog.getLastIndex();
                Optional<Integer> term = raftLog.getTerm(lastIndex);

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
