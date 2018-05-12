package raft.server;

import raft.server.proto.ConfigChange;
import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
@SuppressWarnings("WeakerAccess")
public class RaftNode{
    private final AtomicBoolean started = new AtomicBoolean(false);
    protected final RaftImpl raft;
    public RaftNode(Config c) {
        this.raft = new RaftImpl(c);
    }

    public CompletableFuture<RaftResponse> transferLeader(String transfereeId) {
        return raft.proposeTransferLeader(transfereeId);
    }

    public CompletableFuture<RaftResponse> propose(List<byte[]> data) {
        return raft.proposeData(data);
    }

    public CompletableFuture<RaftResponse> addNode(String newNode) {
        return raft.proposeConfigChange(newNode, ConfigChange.ConfigChangeAction.ADD_NODE);
    }

    public CompletableFuture<RaftResponse> removeNode(String newNode) {
        if (newNode.equals(raft.getLeaderId())) {
            return CompletableFuture.completedFuture(
                    new RaftResponse(raft.getLeaderId(), ErrorMsg.FORBID_REMOVE_LEADER));
        }

        return raft.proposeConfigChange(newNode, ConfigChange.ConfigChangeAction.REMOVE_NODE);
    }

    public void appliedTo(int appliedTo) {
        raft.appliedTo(appliedTo);
    }

    public void receiveCommand(RaftCommand cmd) {
        raft.queueReceivedCommand(cmd);
    }

    public RaftStatus getStatus() {
        return raft.getStatus();
    }

    public String getId() {
        return raft.getSelfId();
    }

    public boolean isLeader() {
        return raft.isLeader();
    }

    public State getState() {
        return raft.getState();
    }

    public void start() {
        if (started.compareAndSet(false, true)) {
            raft.start();
        }
    }

    public void shutdown() {
        if (started.compareAndSet(true, false)) {
            raft.shutdown();
        }
    }
}
