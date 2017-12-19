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
class Candidate extends RaftState {
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class.getName());

    private final int maxElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.max.election.timeout.millis", "300"));
    private ScheduledFuture electionTimeoutFuture;

    Candidate(RaftServer server, ScheduledExecutorService timer) {
        super(server, timer, State.CANDIDATE);
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
                    logger.warn("election timeout, start reelection");
                    this.startElection();
                }
                , electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        final int candidateTerm = this.server.getTerm() + 1;
        final ConcurrentHashMap<String, RemoteRaftClient> clients = this.server.getConnectedClients();
        final int clientsSize = clients.size();
        final int votesNeedToWinLeader = clientsSize / 2;

        final AtomicInteger votesGot = new AtomicInteger();
        RequestVoteCommand vote = new RequestVoteCommand(candidateTerm);
        vote.setCandidateId(server.getId());

        RemotingCommand cmd = RemotingCommand.createRequestCommand(vote);

        for (final RemoteRaftClient client : clients.values()) {
            client.send(cmd, req -> {
                final RemotingCommand res = req.getResponse();
                if (res != null) {
                    final RequestVoteCommand voteRes = new RequestVoteCommand(res.getBody());
                    if (voteRes.getTerm() == candidateTerm &&
                            voteRes.isVoteGranted() &&
                            votesGot.incrementAndGet() > votesNeedToWinLeader &&
                            this.server.getState() != State.LEADER) {
                        this.server.tryTransitStateToLeader(candidateTerm);
                    }
                } else {
                    logger.error("no response returned for request vote ");
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
