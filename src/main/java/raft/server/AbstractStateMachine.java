package raft.server;

import raft.server.proto.RaftCommand;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
@SuppressWarnings("WeakerAccess")
public abstract class AbstractStateMachine implements StateMachine{

    private RaftServer raftServer;
    public AbstractStateMachine(Config c) {
        List<String> peers = c.peers;
        this.raftServer = new RaftServer(c, this);
    }

    public ProposeResponse propose(List<byte[]> data) {
        return raftServer.propose(data);
    }

    @Override
    public void appliedTo(int appliedTo) {
        raftServer.appliedTo(appliedTo);
    }

    @Override
    public void onReceiveCommand(RaftCommand cmd) {
        raftServer.processReceivedCommand(cmd);
    }

    public RaftStatus getStatus() {
        return raftServer.getStatus();
    }

    public boolean isLeader() {
        return raftServer.isLeader();
    }

    public void start() {
        raftServer.start();
    }

    @Override
    public void finish() {
        raftServer.shutdown();
    }
}
