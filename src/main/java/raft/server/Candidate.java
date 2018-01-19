package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.PendingRequest;
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

    private void cleanPendingRequestVotes() {
        for (final Map.Entry<Integer, Future<Void>> e : this.pendingRequestVote.entrySet()) {
            e.getValue().cancel(true);
            this.pendingRequestVote.remove(e.getKey());
        }
    }

    private void startElection() {
        this.server.lockStateLock();
        try {
            assert this.server.getState() == State.CANDIDATE;

            this.cleanPendingRequestVotes();
            final int candidateTerm = this.server.increaseTerm();

            // got self vote initially
            final int votesToWin = this.server.getQuorum() - 1;

            final AtomicInteger votesGot = new AtomicInteger();
            RequestVoteCommand vote = new RequestVoteCommand(candidateTerm, this.server.getId());

            logger.debug("start election candidateTerm={}, votesToWin={}, server={}",
                    candidateTerm, votesToWin, this.server);
            for (final RaftPeerNode node : this.server.getPeerNodes().values()) {
                final RemotingCommand cmd = RemotingCommand.createRequestCommand(vote);
                Future<Void> f = node.send(cmd, (PendingRequest req, RemotingCommand res) -> {
                    if (res.getBody().isPresent()) {
                        final RequestVoteCommand voteRes = new RequestVoteCommand(res.getBody().get());
                        logger.debug("receive request vote response={} from={}", voteRes, node);
                        if (voteRes.getTerm() > this.server.getTerm()) {
                            this.server.tryBecomeFollower(voteRes.getTerm(), voteRes.getFrom());
                        } else {
                            if (voteRes.isVoteGranted() && votesGot.incrementAndGet() >= votesToWin) {
                                this.server.tryBecomeLeader(candidateTerm);
                            }
                        }
                    } else {
                        logger.error("no valid response returned for request vote: {}", cmd.toString());
                    }
                });
                this.pendingRequestVote.put(cmd.getRequestId(), f);
            }

            this.scheduleElectionTimeoutJob();
        } finally {
            this.server.releaseStateLock();
        }
    }

    public void finish() {
        logger.debug("finish candidate, server={}", this.server);
        if (this.electionTimeoutFuture != null) {
            this.electionTimeoutFuture.cancel(false);
            this.electionTimeoutFuture = null;
        }
        this.cleanPendingRequestVotes();
    }
}
