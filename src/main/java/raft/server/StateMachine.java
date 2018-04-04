package raft.server;

import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine extends LifeCycle{
    void onWriteCommand(RaftCommand cmd);
    void onProposalApplied(List<LogEntry> msgs);

    void receiveCommand(RaftCommand cmd);
    CompletableFuture<ProposeResponse> propose(List<byte[]> data);
    void appliedTo(int appliedTo);

    String getId();
    boolean isLeader();
    RaftStatus getStatus();
}
