package raft.server;

import raft.server.proto.LogEntry;
import raft.server.proto.RaftCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine extends LifeCycle{
    void onWriteCommand(RaftCommand cmd);
    void onReceiveCommand(RaftCommand cmd);
    void onProposalApplied(List<LogEntry> msgs);

    ProposeResponse propose(List<byte[]> data);
    void appliedTo(int appliedTo);

    boolean isLeader();
    RaftStatus getStatus();
}
