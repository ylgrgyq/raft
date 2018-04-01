package raft.server;

import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public abstract class AbstractStateMachine implements StateMachine{

    private RaftServer raftServer;
    public AbstractStateMachine(Config c) {
        List<String> peers = c.peers;
        this.raftServer = new RaftServer(c);
    }

    void propose(byte[] data) {
        ArrayList<byte[]> entries = new ArrayList<>();
        entries.add(data);
        raftServer.propose(entries);
    }

    @Override
    public void onReceiveCommand(RaftCommand cmd) {

    }
}
