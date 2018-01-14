package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
class Candidate extends RaftState<RaftServerCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class.getName());

    private final int maxElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.max.election.timeout.millis", "5000"));
    private final int minElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.min.election.timeout.millis", "1000"));
    private ScheduledFuture electionTimeoutFuture;
    private Map<Integer, Future<Void>> pendingRequestVote = new ConcurrentHashMap<>();

    Candidate(RaftServer server, ScheduledExecutorService timer) {
        super(server, timer, State.CANDIDATE);
    }

    public void start() {
        logger.debug("start candidate, server={}", this.server);
        startElection();
    }

    private void scheduleElectionTimeoutJob() {
        if (this.electionTimeoutFuture != null) {
            this.electionTimeoutFuture.cancel(true);
        }

        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int electionTimeoutMillis = random.nextInt(this.minElectionTimeoutMillis, this.maxElectionTimeoutMillis);

        this.electionTimeoutFuture = this.timer.schedule(() -> {
                    logger.warn("election timeout, start reelection, server={}", this.server);
                    this.startElection();
                }
                , electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void cleanPendingRequestVotes(){
        for (final Map.Entry<Integer, Future<Void>> e: this.pendingRequestVote.entrySet()) {
            e.getValue().cancel(true);
            this.pendingRequestVote.remove(e.getKey());
        }
    }

    private void startElection() {
        this.server.lockStateLock();
        try {
            if (this.server.getState() == State.CANDIDATE) {
                this.cleanPendingRequestVotes();
                final int candidateTerm = this.server.increaseTerm();
                final ConcurrentHashMap<String, RaftPeerNode> clients = this.server.getConnectedClients();
                final int clientsSize = clients.size();
                final int votesToWin = this.server.getQuorum();

                final AtomicInteger votesGot = new AtomicInteger();
                RequestVoteCommand vote = new RequestVoteCommand(candidateTerm);
                vote.setCandidateId(server.getId());

                logger.debug("start election candidateTerm={}, votesToWin={}, clientsSize={}, server={}",
                        candidateTerm, votesToWin, clientsSize, this.server);
                for (final RaftPeerNode node : clients.values()) {
                    final RemotingCommand cmd = RemotingCommand.createRequestCommand(vote);
                    Future<Void> f = node.send(cmd, req -> {
                        final RemotingCommand res = req.getResponse();
                        if (res != null) {
                            final RequestVoteCommand voteRes = new RequestVoteCommand(res.getBody());
                            logger.debug("receive request vote response={} from={}", voteRes, node);
                            assert voteRes.getTerm() == candidateTerm;
                            if (voteRes.isVoteGranted() && votesGot.incrementAndGet() >= votesToWin) {
                                this.server.tryTransitStateToLeader(candidateTerm);
                            }
                        } else {
                            logger.error("no response returned for request vote");
                            logger.error(cmd.toString());
                        }
                    });
                    this.pendingRequestVote.put(cmd.getRequestId(), f);
                }

                this.scheduleElectionTimeoutJob();
            }
        } finally {
            this.server.releaseStateLock();
        }
    }

    public void finish() {
        logger.debug("finish candidate, server={}", this.server);
        if (this.electionTimeoutFuture != null) {
            this.electionTimeoutFuture.cancel(true);
            this.electionTimeoutFuture = null;
        }
        this.cleanPendingRequestVotes();
    }
}
