package raft.server;

import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractStateMachine implements StateMachine{
    private final AtomicBoolean started = new AtomicBoolean(false);
    protected final RaftServer raftServer;
    public AbstractStateMachine(Config c) {
        this.raftServer = new RaftServer(c, this);
    }

    public CompletableFuture<ProposeResponse> propose(List<byte[]> data) {
        return raftServer.propose(data);
    }

    @Override
    public void appliedTo(int appliedTo) {
        raftServer.appliedTo(appliedTo);
    }

    @Override
    public void receiveCommand(RaftCommand cmd) {
        raftServer.queueReceivedCommand(cmd);
    }

    public RaftStatus getStatus() {
        return raftServer.getStatus();
    }

    public String getId() {
        return raftServer.getSelfId();
    }

    public boolean isLeader() {
        return raftServer.isLeader();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            raftServer.start();
        }
    }

    @Override
    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            raftServer.shutdown();
        }
    }
}
