package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.rpc.PendingRequest;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
class Candidate extends RaftState<RaftServerCommand> {
    private static final Logger logger = LoggerFactory.getLogger(Candidate.class.getName());
    private Map<Integer, Future<Void>> pendingRequestVote = new ConcurrentHashMap<>();

    Candidate(RaftServer server) {
        super(server, State.CANDIDATE);
    }

    public void start() {
        logger.debug("start candidate, server={}", this.server);
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
        } finally {
            this.server.releaseStateLock();
        }
    }

    @Override
    public void processTickTimeout(long currentTick) {
        this.startElection();
    }

    public void finish() {
        logger.debug("finish candidate, server={}", this.server);
    }
}
