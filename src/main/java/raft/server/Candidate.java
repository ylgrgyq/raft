package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteRaftClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.RemotingCommand;
import raft.server.rpc.RequestVoteCommand;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/12/8
 */
public class Candidate extends RaftState {
    private Logger logger = LoggerFactory.getLogger(Candidate.class.getName());

    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor();
    private final int maxElectionTimeoutMillis = Integer.parseInt(System.getProperty("raft.server.max.election.timeout.millis", "300"));
    private RaftServer server;
    private ScheduledFuture electionTimeoutFuture;

    public void start() {
        startElection();
    }

    private void scheduleElectionTimeoutJob() {
        if (this.electionTimeoutFuture != null) {
            this.electionTimeoutFuture.cancel(true);
        }

        ThreadLocalRandom random = ThreadLocalRandom.current();
        int electionTimeoutMillis = random.nextInt(this.maxElectionTimeoutMillis);

        this.electionTimeoutFuture = this.timer.schedule(this::startElection
                , electionTimeoutMillis, TimeUnit.MILLISECONDS);
    }

    private void startElection() {
        // increase server term
        // send request vote to all clients
        // in request callback set transferToLeader

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
                                this.transferToLeader();
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
    }

    void transferToCandidate(){
        finish();
        // set server state to candidate
    }

    void transferToLeader() {
        finish();
    }

    void transferToFollower() {
        finish();
    }

    public void onReceiveAppendEntries(AppendEntriesCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        } else {

        }
    }

    public void onReceiveRequestVote(RequestVoteCommand cmd) {
        if (cmd.getTerm() > server.getTerm()) {
            // tranfer to follower
        } else {

        }
    }
}
