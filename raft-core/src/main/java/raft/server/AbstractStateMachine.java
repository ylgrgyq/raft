package raft.server;

import raft.server.proto.LogSnapshot;

import java.util.Optional;

public abstract class AbstractStateMachine implements StateMachine{
    @Override
    public void onNodeAdded(RaftStatusSnapshot status, String peerId) {

    }

    @Override
    public void onNodeRemoved(RaftStatusSnapshot status, String peerId) {

    }

    @Override
    public void onLeaderStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onLeaderFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void onFollowerStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onFollowerFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void onCandidateStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onCandidateFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void onShutdown() {

    }
}
