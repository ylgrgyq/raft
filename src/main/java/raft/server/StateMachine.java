package raft.server;

import raft.server.proto.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface StateMachine {
    void onWriteCommand(RaftCommand cmd);
    void onReceiveCommand(RaftCommand cmd);
}
