package raft.server;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.connections.RemoteRaftClient;
import raft.server.processor.AppendEntriesProcessor;
import raft.server.processor.RaftServerCommandListener;
import raft.server.processor.RequestVoteProcessor;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.CommandCode;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final RaftState leader = new Leader(this, timer);
    private final RaftState candidate = new Candidate(this, timer);
    private final RaftState<AppendEntriesCommand> follower = new Follower(this, timer);
    private final ReentrantLock stateLock = new ReentrantLock();

    private ExecutorService processorExecutorService;
    private AtomicInteger term;
    private String selfId;
    private String leaderId;
    private RaftState state;
    private RemoteServer remoteServer;

    private RaftServer(String selfId, EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
                       long clientReconnectDelayMillis, State state) {
        this.term = new AtomicInteger(0);
        this.selfId = selfId;
        this.remoteServer = new RemoteServer(bossGroup, workerGroup, port, clientReconnectDelayMillis);

        switch (state) {
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

    public boolean tryTransitStateToFollower(int term, String leaderId) {
        this.stateLock.lock();
        try {
            if (term > this.term.get()) {
                this.leaderId = leaderId;
                this.term.set(term);
                this.transitState(follower);
                return true;
            } else {
                return false;
            }
        } finally {
            this.stateLock.unlock();
        }
    }

    boolean tryTransitStateToLeader(int term) {
        this.stateLock.lock();
        try {
            if (this.getState() == State.CANDIDATE &&
                    term > this.term.get()) {
                this.leaderId = this.selfId;
                this.term.set(term);
                this.transitState(leader);
                return true;
            } else {
                return false;
            }
        } finally {
            this.stateLock.unlock();
        }
    }

    boolean tryTransitStateToCandidate() {
        this.stateLock.lock();
        try {
            if (this.getState() == State.FOLLOWER) {
                this.leaderId = null;
                this.transitState(candidate);
                return true;
            } else {
                logger.error("transient state to candidate failed, {} {} {}", this.term, this.getState(), this.leaderId);
                return false;
            }
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
        List<RaftServerCommandListener<AppendEntriesCommand>> listeners = new ArrayList<>();
        listeners.add(this.follower);
        this.remoteServer.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this, listeners), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
    }

    void start(List<InetSocketAddress> clientAddrs) throws InterruptedException {
        this.remoteServer.startLocalServer();
        this.remoteServer.connectToClients(clientAddrs);

        this.stateLock.lock();
        try {
            this.state.start();
        } finally {
            this.stateLock.unlock();
        }
    }

    public int getTerm() {
        return this.term.get();
    }

    int increaseTerm() {
        this.stateLock.lock();
        try {
            return this.term.incrementAndGet();
        }finally {
            this.stateLock.unlock();
        }
    }

    String getId() {
        return this.selfId;
    }

    ConcurrentHashMap<String, RemoteRaftClient> getConnectedClients() {
        return this.remoteServer.getConnectedClients();
    }

    State getState() {
        this.stateLock.lock();
        try {
            return state.getState();
        } finally {
            this.stateLock.unlock();
        }
    }

    void lockStateLock() {
        this.stateLock.lock();
    }

    void releaseStateLock(){
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
        this.timer.shutdown();
    }

    public String getLeaderId() {
        return leaderId;
    }

    static class RaftServerBuilder {
        private long clientReconnectDelayMillis = 3000;

        private EventLoopGroup bossGroup;
        private EventLoopGroup workerGroup;
        private int port;
        private State state;

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

        RaftServerBuilder withClientReconnectDelay(long delay, TimeUnit timeUnit) {
            this.clientReconnectDelayMillis = timeUnit.toMillis(delay);
            return this;
        }

        RaftServer build() throws UnknownHostException {
            String selfId = InetAddress.getLocalHost().getHostAddress() + ":" + this.port;
            return new RaftServer(selfId, this.bossGroup, this.workerGroup, this.port,
                    this.clientReconnectDelayMillis, this.state);
        }
    }
}
