package raft.server;

import io.netty.channel.EventLoopGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.connections.RemoteRaftClient;
import raft.server.processor.AppendEntriesProcessor;
import raft.server.processor.RequestVoteProcessor;
import raft.server.rpc.CommandCode;
import raft.server.state.Candidate;
import raft.server.state.Follower;
import raft.server.state.Leader;
import raft.server.state.RaftState;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public class RaftServer {
    private static final Logger logger = LoggerFactory.getLogger(RaftServer.class.getName());

    private final Map<State, RaftState> stateHandlerMap;

    private ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    private ExecutorService processorExecutorService;
    private AtomicInteger term;
    private String selfId;
    private String leaderId;
    private State state;
    private RemoteServer remoteServer;

    private RaftServer(String selfId, EventLoopGroup bossGroup, EventLoopGroup workerGroup, int port,
                       long clientReconnectDelayMillis, State state) {
        this.term = new AtomicInteger(0);
        this.state = state;
        this.selfId = selfId;
        this.remoteServer = new RemoteServer(bossGroup, workerGroup, port, clientReconnectDelayMillis);

        this.stateHandlerMap = new HashMap<>();
        this.stateHandlerMap.put(State.LEADER, new Leader(this));
        this.stateHandlerMap.put(State.CANDIDATE, new Candidate(this));
        this.stateHandlerMap.put(State.FOLLOWER, new Follower(this));
    }

    public boolean transitStateToFollower(int term, String leaderId) {
        this.stateLock.writeLock().lock();
        try {
            if (term > this.term.get()) {
                this.transitState(State.FOLLOWER);
                this.leaderId = leaderId;
                return true;
            } else {
                return false;
            }
        } finally {
            this.stateLock.writeLock().unlock();
        }
    }

    public void transitState(State nextState) {
        try {
            this.stateLock.writeLock().lockInterruptibly();
            try {
                if (this.getState() != nextState) {
                    State currentState = this.getState();
                    RaftState prevHandler = this.stateHandlerMap.get(currentState);
                    prevHandler.finish();

                    this.state = nextState;
                    RaftState nextHandler = this.stateHandlerMap.get(nextState);
                    nextHandler.start();
                }
            } finally {
                this.stateLock.writeLock().unlock();
            }
        } catch (InterruptedException ex) {
            logger.warn("got interruption in transiting state to {}", nextState.name());
        }
    }



    private void registerProcessors() {
        this.remoteServer.registerProcessor(CommandCode.APPEND_ENTRIES, new AppendEntriesProcessor(this), this.processorExecutorService);
        this.remoteServer.registerProcessor(CommandCode.REQUEST_VOTE, new RequestVoteProcessor(this), this.processorExecutorService);
    }

    void start(List<InetSocketAddress> clientAddrs) throws InterruptedException {
        this.remoteServer.startLocalServer();

        this.remoteServer.connectToClients(clientAddrs);
        this.stateHandlerMap.get(this.state).start();
    }

    public int increaseTerm(){
        return this.term.incrementAndGet();
    }

    public int getTerm() {
        return this.term.get();
    }

    public String getId() {
        return this.selfId;
    }

    public ConcurrentHashMap<String, RemoteRaftClient> getConnectedClients() {
        return this.remoteServer.getConnectedClients();
    }

    public State getState() {
        this.stateLock.readLock().lock();
        try {
            return state;
        } finally {
            this.stateLock.readLock().unlock();
        }
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
    }

    public String getLeaderId() {
        return leaderId;
    }

    public enum State {
        FOLLOWER,
        CANDIDATE,
        LEADER
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
