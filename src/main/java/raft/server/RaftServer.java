package raft.server;

import static com.google.common.base.Preconditions.*;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.Pair;
import raft.ThreadFactoryImpl;
import raft.server.connections.NettyRemoteClient;
import raft.server.connections.NettyRemoteServer;
import raft.server.log.RaftLog;
import raft.server.processor.AppendEntriesProcessor;
import raft.server.processor.ClientRequestProcessor;
import raft.server.processor.RaftCommandListener;
import raft.server.processor.RequestVoteProcessor;
import raft.server.rpc.*;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

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
    private final NettyRemoteServer remoteServer;
    private final NettyRemoteClient remoteClient;
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

    private RaftServer(RaftServerBuilder builder) {
        this.term = new AtomicInteger(0);
        this.selfId = builder.selfId;
        this.remoteServer = new NettyRemoteServer(builder.bossGroup, builder.workerGroup, builder.port);
        this.remoteClient = new NettyRemoteClient(builder.workerGroup, builder.clientReconnectDelayMs);
        this.raftLog = new RaftLog();
        this.tickIntervalMs = builder.tickIntervalMs;
        this.electionTimeoutTicks = this.generateElectionTimeoutTicks();

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

    private boolean tryBecomeLeader(int term) {
        this.stateLock.lock();
        try {
            if (this.getState() == State.CANDIDATE &&
                    term <= this.term.get()) {
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
        this.remoteServer.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.CLIENT_REQUEST, new ClientRequestProcessor(this), this.processorExecutorService);
    }

    void start(List<String> clientAddrs) throws InterruptedException, ExecutionException, TimeoutException {
        checkNotNull(clientAddrs);
        checkArgument((clientAddrs.size() & 0x1) != 0, "need odd number of raft servers in a raft cluster");

        this.registerProcessors();

        this.remoteServer.startLocalServer();
        this.remoteClient.connectToClients(clientAddrs);
        for (String id : clientAddrs) {
            // initial next index to arbitrary value
            // we'll reset this value to last index log when this raft server become leader
            this.peerNodes.put(id, new RaftPeerNode(id, this, this.raftLog, this.remoteClient, 1));
        }

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
                            this.state.processTickTimeout();
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

    private boolean appendLogOnLeader(LogEntry entry) {
        this.stateLock.tryLock();
        try {
            if (this.getState() == State.LEADER) {
                try {
                    entry.setTerm(this.getTerm());
                    ArrayList<LogEntry> entries = new ArrayList<>();
                    entries.add(entry);
                    this.raftLog.append(entries);
                    this.broadcastAppendEntries();
                    return true;
                } catch (Throwable t) {
                    logger.error("append log failed {}", entry, t);
                }
            }
        } finally {
            this.stateLock.unlock();
        }

        return false;
    }

    public Pair<Boolean, String> appendFromClient(LogEntry entry) {
        if (this.getState() == State.LEADER) {
            return Pair.of(this.appendLogOnLeader(entry), this.getLeaderId());
        } else {
            return Pair.of(false, this.getLeaderId());
        }
    }

    public boolean appendLogsOnFollower(int prevIndex, int prevTerm, int leaderCommitId, String leaderId, List<LogEntry> entries) {
        this.stateLock.tryLock();
        try {
            State state = this.getState();
            if (state == State.FOLLOWER) {
                try {
                    this.setLeaderId(leaderId);
                    return raftLog.tryAppendEntries(prevIndex, prevTerm, leaderCommitId, entries);
                } catch (Exception ex) {
                    logger.error("append entries failed, leaderCommitId={}, leaderId, entries",
                            leaderCommitId, leaderId, entries, ex);
                }
            } else {
                logger.error("append logs failed due to invalid state: {}", state);
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
                peer.sendAppend(RaftServer.maxMsgSize);
            }
        }
    }

    void updateCommit(){
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

    void shutdown() {
        this.remoteServer.shutdown();
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
        public void processTickTimeout() {
            RaftServer.this.broadcastAppendEntries();
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
        public void processTickTimeout() {
            assert RaftServer.this.stateLock.isLocked();

            logger.info("election timeout, become candidate");

            this.becomeCandidate();
        }
    }

    class Candidate extends RaftState {
        private Map<Integer, Future<Void>> pendingRequestVote = new ConcurrentHashMap<>();

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
            assert RaftServer.this.getState() == State.CANDIDATE;

            this.cleanPendingRequestVotes();
            final int candidateTerm = RaftServer.this.term.incrementAndGet();

            // got self vote initially
            final int votesToWin = RaftServer.this.getQuorum() - 1;

            if (votesToWin == 0) {
                RaftServer.this.tryBecomeLeader(candidateTerm);
            } else {
                final AtomicInteger votesGot = new AtomicInteger();
                RequestVoteCommand vote = new RequestVoteCommand(candidateTerm, RaftServer.this.getId());
                Optional<LogEntry> lastEntry = RaftServer.this.raftLog.getEntry(RaftServer.this.raftLog.getLastIndex());
                assert lastEntry.isPresent();

                LogEntry e = lastEntry.get();
                vote.setLastLogIndex(e.getIndex());
                vote.setLastLogTerm(e.getTerm());

                logger.debug("start election candidateTerm={}, votesToWin={}, server={}, voteCmd={}",
                        candidateTerm, votesToWin, RaftServer.this);
                for (final RaftPeerNode node : RaftServer.this.getPeerNodes().values()) {
                    final RemotingCommand cmd = RemotingCommand.createRequestCommand(vote);
                    Future<Void> f = node.send(cmd, (PendingRequest req, RemotingCommand res) -> {
                        if (res.getBody().isPresent()) {
                            final RequestVoteCommand voteRes = new RequestVoteCommand(res.getBody().get());
                            logger.debug("receive request vote response={} from={}", voteRes, node);
                            if (voteRes.getTerm() > RaftServer.this.getTerm()) {
                                RaftServer.this.tryBecomeFollower(voteRes.getTerm(), voteRes.getFrom());
                            } else {
                                if (voteRes.isVoteGranted() && votesGot.incrementAndGet() >= votesToWin) {
                                    RaftServer.this.tryBecomeLeader(candidateTerm);
                                }
                            }
                        } else {
                            logger.error("no valid response returned for request vote {}. maybe request timeout", cmd.toString());
                        }
                    });
                    this.pendingRequestVote.put(cmd.getRequestId(), f);
                }
            }
        }

        @Override
        public boolean isTickTimeout(long currentTick) {
            return currentTick >= RaftServer.this.electionTimeoutTicks;
        }

        @Override
        public void processTickTimeout() {
            this.startElection();
        }
    }

    @SuppressWarnings("unused")
    static class RaftServerBuilder {
        private long clientReconnectDelayMs = 1000;
        private long tickIntervalMs = 100;

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private int port;
        private String selfId;
        private String leaderId;

        RaftServerBuilder withBossGroup(EventLoopGroup group) {
            checkNotNull(group);
            this.bossGroup = group;
            return this;
        }

        RaftServerBuilder withWorkerGroup(EventLoopGroup group) {
            checkNotNull(group);
            this.workerGroup = group;
            return this;
        }

        RaftServerBuilder withServerPort(int port) {
            this.port = port;
            return this;
        }

        RaftServerBuilder withTickInterval(long tickInterval, TimeUnit unit) {
            this.tickIntervalMs = unit.toMillis(tickInterval);
            checkArgument(this.tickIntervalMs >= 100,
                    "raft server tick interval should >= 100ms");
            return this;
        }

        RaftServerBuilder withClientReconnectDelay(long delay, TimeUnit timeUnit) {
            checkArgument(delay > 0);
            this.clientReconnectDelayMs = timeUnit.toMillis(delay);
            return this;
        }

        RaftServer build() throws UnknownHostException {
            if (this.bossGroup == null) {
                this.bossGroup = new NioEventLoopGroup(1);
            }

            if (this.workerGroup == null) {
                this.workerGroup = new NioEventLoopGroup();
            }

            this.selfId = InetAddress.getLocalHost().getHostAddress() + ":" + this.port;
            return new RaftServer(this);
        }
    }
}
