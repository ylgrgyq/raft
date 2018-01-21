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
import raft.server.rpc.CommandCode;
import raft.server.rpc.RaftCommand;
import raft.server.rpc.RaftServerCommand;

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

    // TODO better use ThreadPoolExecutor to name every thread pool
    private final ScheduledExecutorService tickGenerator = Executors.newSingleThreadScheduledExecutor();
    private final ExecutorService tickTimeoutExecutors = Executors.newFixedThreadPool(8);

    private final RaftState<RaftServerCommand> leader = new Leader(this);
    private final RaftState<RaftServerCommand> candidate = new Candidate(this);
    private final RaftState<RaftCommand> follower = new Follower(this);
    private final ReentrantLock stateLock = new ReentrantLock();
    private final ConcurrentHashMap<String, RaftPeerNode> peerNodes = new ConcurrentHashMap<>();
    private final AtomicLong tickCount = new AtomicLong();

    private List<String> peerNodeIds = Collections.emptyList();
    private ExecutorService processorExecutorService;

    // TODO need persistent
    private String voteFor = null;

    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private AtomicInteger term;
    private final long tickIntevalMs;
    private final String selfId;
    private final RemoteServer remoteServer;
    private final RaftLog raftLog;
    private TickTimeoutProcessor tickfn;
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

    boolean tryBecomeLeader(int term) {
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

    boolean tryBecomeCandidate() {
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

    int increaseTerm() {
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

    ConcurrentHashMap<String, RaftPeerNode> getPeerNodes() {
        return this.peerNodes;
    }

    int getQuorum() {
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

    void broadcastAppendEntries() {
        for (final RaftPeerNode peer : peerNodes.values()) {
            peer.sendAppend();
        }
    }

    void lockStateLock() {
        this.stateLock.lock();
    }

    void releaseStateLock() {
        this.stateLock.unlock();
    }

    void initialize() throws Exception {
        int processorThreadPoolSize = Integer.parseInt(System.getProperty("raft.server.processor.thread.pool.size", "8"));
        this.processorExecutorService = Executors.newFixedThreadPool(processorThreadPoolSize,
                new ThreadFactoryImpl("RaftServerProcessorThread_"));

        this.registerProcessors();
    }

    void shutdown() {
        this.remoteServer.shutdown();
        this.processorExecutorService.shutdown();
        this.tickGenerator.shutdown();
    }

    @Override
    public void onReceiveRaftCommand(RaftServerCommand cmd) {
        switch (this.getState()) {
            case CANDIDATE:
            case FOLLOWER:
                this.clearTickCount();
        }
    }

    interface TickTimeoutProcessor {
        void processTickTimeout(long currentTick);
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
