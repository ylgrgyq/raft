package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.RaftLog;
import raft.server.processor.RaftCommandListener;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;
import raft.server.rpc.RaftServerCommand;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer implements RaftCommandListener<RaftServerCommand> {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    private final static long pingIntervalTicks = Long.parseLong(System.getProperty("raft.server.leader.ping.interval.ticks", "20"));
    private final static long suggestElectionTimeoutTicks = Long.parseLong(System.getProperty("raft.server.suggest.election.timeout.ticks", "40"));
    private final static int maxMsgSize = Integer.parseInt(System.getProperty("raft.server.read.raft.log.max.msg.size", "16"));
    private final static int processorThreadPoolSize = Integer.parseInt(System.getProperty("raft.server.processor.thread.pool.size", "8"));

    // TODO better use ThreadPoolExecutor to give every thread pool a name
    private final ScheduledExecutorService tickGenerator = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService tickTimeoutExecutors = Executors.newFixedThreadPool(8);
    private final ExecutorService processorExecutorService = Executors.newFixedThreadPool(processorThreadPoolSize,
            new ThreadFactoryImpl("RaftServerProcessorThread_"));
    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();

    private final String selfId;
    private final RaftLog raftLog;
    private final long tickIntervalMs;

    private List<String> peerNodeIds = Collections.emptyList();

    // TODO need persistent
    private String voteFor;
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private AtomicInteger term;
    private String leaderId;
    private RaftState state;
    private long matchIndex;
    private long electionTimeoutTicks;
    private StateMachine stateMachine;

    public RaftServer(Config c) {
        this.term = new AtomicInteger(0);
        this.selfId = c.selfId;
        this.raftLog = new RaftLog();
        this.tickIntervalMs = c.tickIntervalMs;
        this.electionTimeoutTicks = this.generateElectionTimeoutTicks();
        this.stateMachine = c.stateMachine;

        this.state = follower;
        this.reset();
    }

    private void reset() {
        this.voteFor = null;
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

    public boolean tryBecomeFollower(int term, String leaderId) {
        this.stateLock.lock();
        try {
            if (term > this.term.get()) {
                this.leaderId = leaderId;
                this.term.set(term);
                this.reset();
                this.transitState(follower);
                return true;
            } else {
                logger.warn("transient state to {} failed, term={} leaderId={} server={}", State.FOLLOWER, term, leaderId, this.toString());
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
                    node.sendAppend(RaftServer.maxMsgSize);
                }
                return true;
            } else {
                logger.warn("transient state to {} failed, term={} server={}", State.LEADER, term, this.toString());
                return false;
            }
        } finally {
            this.stateLock.unlock();
        }
    }

    private void registerProcessors() {
//        this.remoteServer.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
//        this.remoteServer.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
//        this.remoteServer.registerProcessor(CommandCode.CLIENT_REQUEST, new ClientRequestProcessor(this), this.processorExecutorService);
    }

    void start(List<String> clientAddrs) throws InterruptedException, ExecutionException, TimeoutException {
        checkNotNull(clientAddrs);
        checkArgument((clientAddrs.size() & 0x1) != 0, "need odd number of raft servers in a raft cluster");

        this.registerProcessors();

//        this.remoteServer.startLocalServer();
//        this.remoteClient.connectToClients(clientAddrs);
//        for (String id : clientAddrs) {
            // initial next index to arbitrary value
            // we'll reset this value to last index log when this raft server become leader
//            this.peerNodes.put(id, new RaftPeerNode(id, this, this.raftLog, this.remoteClient, 1));
//        }

        this.peerNodeIds = Collections.unmodifiableList(clientAddrs);

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

    public String getId() {
        return this.selfId;
    }

    public String getVoteFor() {
        this.stateLock.lock();
        try {
            return voteFor;
        } finally {
            this.stateLock.unlock();
        }
    }

    public void setVoteFor(String voteFor) {
        this.stateLock.lock();
        try {
            this.voteFor = voteFor;
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
        return RaftServer.suggestElectionTimeoutTicks +
                ThreadLocalRandom.current().nextLong(RaftServer.suggestElectionTimeoutTicks);
    }

    private ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return this.peerNodes;
    }

    private int getQuorum() {
        return Math.max(2, this.peerNodeIds.size() / 2 + 1);
    }

    private State getState() {
        return this.state.getState();
    }

    public RaftLog getRaftLog() {
        return this.raftLog;
    }

    public String getLeaderId() {
        return this.leaderId;
    }

    public ProposeResponse propose(List<byte[]> entries) {
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
            logger.error("directAppend log failed {}", entries, t);
            error = ErrorMsg.INTERNAL_ERROR;
        } finally {
            this.stateLock.unlock();
        }

        return new ProposeResponse(leaderId, error);
    }

    public boolean replicateLogsOnFollower(RaftCommand cmd) {
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
                    logger.error("directAppend entries failed, leaderCommitId={}, leaderId, entries",
                            leaderCommitId, leaderId, entries, ex);
                }
            } else {
                logger.error("directAppend logs failed due to invalid state: {}", state);
            }
        } finally {
            this.stateLock.unlock();
        }

        return false;
    }

    private void broadcastAppendEntries() {
        if (peerNodes.isEmpty()) {
            this.raftLog.tryCommitTo(this.raftLog.getLastIndex());
        } else {
            for (final RaftPeerNode peer : peerNodes.values()) {
                if (! peer.getPeerId().equals(this.selfId)) {
                    peer.sendAppend(RaftServer.maxMsgSize);
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
        kthLog.ifPresent(e -> {
            if (e.getTerm() == this.getTerm()) {
                this.raftLog.tryCommitTo(kthMatchedIndexes);
            }
        });
    }

    void processReceivedCommand(raft.server.proto.RaftCommand cmd) {
        // check term
        if (cmd.getTerm() > this.getTerm()) {
            this.tryBecomeFollower(cmd.getTerm(), cmd.getFrom());
        }

        state.process(cmd);
    }

    void shutdown() {
        this.tickGenerator.shutdown();
        this.processorExecutorService.shutdown();
        this.tickTimeoutExecutors.shutdown();
    }

    @Override
    public void onReceiveRaftCommand(RaftServerCommand cmd) {
        switch (this.getState()) {
            case CANDIDATE:
            case FOLLOWER:
                this.clearTickCount();
        }
    }

    @Override
    public String toString() {
        return "RaftServer{" +
                "term=" + term +
                ", selfId='" + selfId + '\'' +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + this.getState() +
                '}';
    }

    class Leader extends RaftState {
        Leader() {
            super(State.LEADER);
        }

        public void start() {
            logger.debug("start leader, server={}", RaftServer.this);
            RaftServer.this.broadcastAppendEntries();
        }

        public void finish() {
            logger.debug("finish leader, server={}", RaftServer.this);
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftServer.pingIntervalTicks;
        }

        @Override
        public void onTickTimeout() {
            RaftServer.this.broadcastAppendEntries();
        }

        @Override
        void process(RaftCommand cmd) {

        }
    }

    class Follower extends RaftState {
        Follower() {
            super(State.FOLLOWER);
        }

        public void start() {
            logger.debug("start follower, server={}", RaftServer.this);
        }

        public void finish() {
            logger.debug("finish follower, server={}", RaftServer.this);
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

            logger.info("election timeout, become candidate");

            this.becomeCandidate();
        }

        @Override
        void process(RaftCommand cmd) {

        }
    }

    class Candidate extends RaftState {
        private Map<Integer, Future<Void>> pendingRequestVote = new ConcurrentHashMap<>();
        private volatile ConcurrentHashMap<String, Boolean> votesGot = new ConcurrentHashMap<>();

        Candidate() {
            super(State.CANDIDATE);
        }

        public void start() {
            logger.debug("start candidate, server={}", RaftServer.this);
            this.startElection();
        }

        public void finish() {
            logger.debug("finish candidate, server={}", RaftServer.this);
            this.cleanPendingRequestVotes();
        }

        private void cleanPendingRequestVotes() {
            for (final Map.Entry<Integer, Future<Void>> e : this.pendingRequestVote.entrySet()) {
                e.getValue().cancel(true);
                this.pendingRequestVote.remove(e.getKey());
            }
        }

        private void startElection() {
            votesGot = new ConcurrentHashMap<>();
            assert RaftServer.this.getState() == State.CANDIDATE;

            this.cleanPendingRequestVotes();
            final int candidateTerm = RaftServer.this.term.incrementAndGet();

            // got self vote initially
            final int votesToWin = RaftServer.this.getQuorum() - 1;

            if (votesToWin == 0) {
                RaftServer.this.tryBecomeLeader();
            } else {
                Optional<LogEntry> lastEntry = RaftServer.this.raftLog.getEntry(RaftServer.this.raftLog.getLastIndex());
                assert lastEntry.isPresent();

                LogEntry e = lastEntry.get();

                logger.debug("start election candidateTerm={}, votesToWin={}, server={}, voteCmd={}",
                        candidateTerm, votesToWin, RaftServer.this);
                for (final RaftPeerNode node : RaftServer.this.getPeerNodes().values()) {
                    if (! node.getPeerId().equals(RaftServer.this.selfId)) {
                        raft.server.proto.RaftCommand vote = raft.server.proto.RaftCommand.newBuilder()
                                .setTerm(candidateTerm)
                                .setFrom(RaftServer.this.getId())
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
