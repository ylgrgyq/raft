package raft.server;

import com.google.common.base.Preconditions;
import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.connections.RemoteClient;
import raft.server.processor.AppendEntriesProcessor;
import raft.server.processor.ClientRequestProcessor;
import raft.server.processor.RaftCommandListener;
import raft.server.processor.RequestVoteProcessor;
import raft.server.rpc.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

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
    private ExecutorService processorExecutorService = Executors.newFixedThreadPool(processorThreadPoolSize,
            new ThreadFactoryImpl("RaftServerProcessorThread_"));
    private final RaftState leader = new Leader();
    private final RaftState candidate = new Candidate();
    private final RaftState follower = new Follower();
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();

    private final String selfId;
    private final RemoteServer remoteServer;
    private final RaftLog raftLog;
    private final long tickIntevalMs;

    private List<String> peerNodeIds = Collections.emptyList();

    // TODO need persistent
    private String voteFor = null;
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private AtomicInteger term;
    private String leaderId;
    private RaftState state;
    private long matchIndex;
    private long nextIndex;
    private long electionTimeoutTicks;

    private RaftServer(RaftServerBuilder builder) {
        this.term = new AtomicInteger(0);
        this.selfId = builder.selfId;
        this.remoteServer = new RemoteServer(builder.bossGroup, builder.workerGroup, builder.port, builder.clientReconnectDelayMs);
        this.raftLog = new RaftLog();
        this.nextIndex = 1;
        this.tickIntevalMs = builder.tickIntervalMs;
        this.electionTimeoutTicks = this.generateElectionTimeoutTicks();

        this.state = follower;
        if (builder.state != null) {
            switch (builder.state) {
                case CANDIDATE:
                    this.state = candidate;
                    break;
                case FOLLOWER:
                    this.state = follower;
                    break;
                case LEADER:
                    this.state = leader;
                    break;
            }
        }
    }

    public boolean tryBecomeFollower(int term, String leaderId) {
        this.stateLock.lock();
        try {
            if (term > this.term.get()) {
                this.leaderId = leaderId;
                this.term.set(term);
                this.voteFor = null;
                this.tickCount.set(0);
                this.electionTimeoutTicks = this.generateElectionTimeoutTicks();
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
                    term == this.term.get()) {
                this.leaderId = this.selfId;
                this.term.set(term);
                this.voteFor = null;
                this.tickCount.set(0);
                this.electionTimeoutTicks = this.generateElectionTimeoutTicks();
                this.transitState(leader);
                for (RaftPeerNode node : this.peerNodes.values()) {
                    node.reset(this.raftLog.lastIndex() + 1);
                    node.sendAppend();
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

    private boolean tryBecomeCandidate() {
        this.stateLock.lock();
        try {
            assert this.getState() == State.FOLLOWER;

            this.leaderId = null;
            this.voteFor = null;
            this.tickCount.set(0);
            this.electionTimeoutTicks = this.generateElectionTimeoutTicks();
            this.transitState(candidate);
            return true;
        } finally {
            this.stateLock.unlock();
        }
    }

    private void transitState(RaftState nextState) {
        if (this.state != nextState) {
            this.state.finish();

            this.state = nextState;
            nextState.start();
        }
    }

    private void registerProcessors() {
        this.remoteServer.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.CLIENT_REQUEST, new ClientRequestProcessor(this), this.processorExecutorService);
    }

    void start(List<InetSocketAddress> clientAddrs) throws InterruptedException, ExecutionException, TimeoutException {
        this.remoteServer.startLocalServer();
        Map<String, RemoteClient> clients = this.remoteServer.connectToClients(clientAddrs, 30, TimeUnit.SECONDS);
        this.peerNodeIds = Collections.unmodifiableList(new ArrayList<>(clients.keySet()));
        for (Map.Entry<String, RemoteClient> c : clients.entrySet()) {
            // initial next index to arbitrary value
            // we'll reset this value to last index log when this raft server become leader
            this.peerNodes.put(c.getKey(), new RaftPeerNode(this, this.raftLog, c.getValue(), 1));
        }

        this.stateLock.lock();
        try {
            this.state.start();
        } finally {
            this.stateLock.unlock();
        }

        this.tickGenerator.scheduleWithFixedDelay(() -> {
            long tick = this.tickCount.incrementAndGet();
            this.tickCount.set(0);

            long timeoutTicks;
            if (this.getState() == State.LEADER) {
                timeoutTicks = RaftServer.pingIntervalTicks;
            } else {
                timeoutTicks = this.electionTimeoutTicks;
            }

            if (tick > timeoutTicks) {
                try {
                    this.tickTimeoutExecutors.submit(() -> {
                        try {
                            this.state.processTickTimeout(tick);
                        } catch (Throwable t) {
                            logger.error("process tick timeout failed on tick {} of state {}", tick, this.state.getState(), t);
                        }
                    });
                } catch (RejectedExecutionException ex) {
                    logger.error("submit process tick timeout job failed", ex);
                }
            }

        }, this.tickIntevalMs, this.tickIntevalMs, TimeUnit.MILLISECONDS);
    }

    public int getTerm() {
        return this.term.get();
    }

    private int increaseTerm() {
        this.stateLock.lock();
        try {
            return this.term.incrementAndGet();
        } finally {
            this.stateLock.unlock();
        }
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

    public void setLeaderId(String leaderId) {
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

    public State getState() {
        this.stateLock.lock();
        try {
            return state.getState();
        } finally {
            this.stateLock.unlock();
        }
    }

    public RaftLog getRaftLog() {
        return raftLog;
    }

    int getMaxMsgSize() {
        return maxMsgSize;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void appendLog(LogEntry entry) {
        this.raftLog.append(this.getTerm(), entry);
        this.broadcastAppendEntries();
    }

    private void broadcastAppendEntries() {
        for (final RaftPeerNode peer : peerNodes.values()) {
            peer.sendAppend();
        }
    }

    private void lockStateLock() {
        this.stateLock.lock();
    }

    private void releaseStateLock() {
        this.stateLock.unlock();
    }

    void initialize() throws Exception {


        this.registerProcessors();
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

    interface TickTimeoutProcessor {
        void processTickTimeout(long currentTick);
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
        public void processTickTimeout(long currentTick) {
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

        public synchronized void finish() {
            logger.debug("finish follower, server={}", RaftServer.this);
        }

        @Override
        public void processTickTimeout(long currentTick) {
            logger.info("election timeout, become candidate");
            try {
                RaftServer.this.tryBecomeCandidate();
            } catch (Exception ex) {
                logger.error("become candidate failed", ex);
            }
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

        private void cleanPendingRequestVotes() {
            for (final Map.Entry<Integer, Future<Void>> e : this.pendingRequestVote.entrySet()) {
                e.getValue().cancel(true);
                this.pendingRequestVote.remove(e.getKey());
            }
        }

        private void startElection() {
            RaftServer.this.lockStateLock();
            try {
                assert RaftServer.this.getState() == State.CANDIDATE;

                this.cleanPendingRequestVotes();
                final int candidateTerm = RaftServer.this.increaseTerm();

                // got self vote initially
                final int votesToWin = RaftServer.this.getQuorum() - 1;

                final AtomicInteger votesGot = new AtomicInteger();
                RequestVoteCommand vote = new RequestVoteCommand(candidateTerm, RaftServer.this.getId());

                logger.debug("start election candidateTerm={}, votesToWin={}, server={}",
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
                            logger.error("no valid response returned for request vote: {}", cmd.toString());
                        }
                    });
                    this.pendingRequestVote.put(cmd.getRequestId(), f);
                }
            } finally {
                RaftServer.this.releaseStateLock();
            }
        }

        @Override
        public void processTickTimeout(long currentTick) {
            this.startElection();
        }

        public void finish() {
            logger.debug("finish candidate, server={}", RaftServer.this);
        }
    }

    @SuppressWarnings("unused")
    static class RaftServerBuilder {
        private long clientReconnectDelayMs = 3000;
        private long tickIntervalMs = 100;

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private int port;
        private State state;
        private String selfId;

        RaftServerBuilder withBossGroup(EventLoopGroup group) {
            this.bossGroup = group;
            return this;
        }

        RaftServerBuilder withWorkerGroup(EventLoopGroup group) {
            this.workerGroup = group;
            return this;
        }

        RaftServerBuilder withServerPort(int port) {
            this.port = port;
            return this;
        }

        RaftServerBuilder withRole(String role) {
            this.state = State.valueOf(role);
            return this;
        }

        RaftServerBuilder withTickInterval(long tickInterval, TimeUnit unit) {
            this.tickIntervalMs = unit.toMillis(tickInterval);
            Preconditions.checkArgument(this.tickIntervalMs >= 100,
                    "processTickTimeout interval should >= 100ms");
            return this;
        }

        RaftServerBuilder withClientReconnectDelay(long delay, TimeUnit timeUnit) {
            this.clientReconnectDelayMs = timeUnit.toMillis(delay);
            return this;
        }

        RaftServer build() throws UnknownHostException {
            this.selfId = InetAddress.getLocalHost().getHostAddress() + ":" + this.port;
            return new RaftServer(this);
        }
    }
}
