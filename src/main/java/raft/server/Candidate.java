package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
public class Candidate extends RaftState {
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final int maxElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.max.election.timeout.millis", "300"));
    private RaftServer server;
    private ScheduledFuture electionTimeoutFuture;

    Candidate(RaftServer server) {
        this.server = server;
    }

    public void start() {
        startElection();
    }

    private void scheduleElectionTimeoutJob() {
        if (this.electionTimeoutFuture != null) {
            this.electionTimeoutFuture.cancel(true);
        }

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int electionTimeoutMillis = random.nextInt(this.maxElectionTimeoutMillis);

        this.electionTimeoutFuture = this.timer.schedule(() -> {
                    logger.warn("election timeout, reelection after {} millis", electionTimeoutMillis);
                    this.startElection();
                }
                , electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        this.server.increaseTerm();

        final ConcurrentHashMap<String, RemoteRaftClient> clients = this.server.getConnectedClients();
        final int clientsSize = clients.size();
        final int votesNeedToWinLeader = clientsSize / 2;

        final AtomicInteger votesGot = new AtomicInteger();
        for (final RemoteRaftClient client : clients.values()) {
            client.requestVote(req -> {
                final RemotingCommand res = req.getResponse();
                if (res != null) {
                    final RequestVoteCommand v = new RequestVoteCommand(res.getBody());
                    if (v.getTerm() == this.server.getTerm() &&
                            v.isVoteGranted() &&
                            votesGot.incrementAndGet() > votesNeedToWinLeader &&
                            this.server.getState() != RaftServer.State.LEADER) {
                        synchronized (this) {
                            final RaftServer.State state = this.server.getState();
                            if (state != RaftServer.State.LEADER) {
                                this.server.transitState(RaftServer.State.LEADER);
                            }
                        }
                    }
                }
            });
        }

        this.scheduleElectionTimeoutJob();
    }

    public void finish() {
        this.electionTimeoutFuture.cancel(true);
        this.electionTimeoutFuture = null;
    }
}
