package raft.server;

import raft.server.proto.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 18/5/9
 */
public interface RaftCommandBroker {
    void onWriteCommand(RaftCommand cmd);
    void onFlushCommand();

    default void onWriteAndFlushCommand(RaftCommand cmd) {
        onWriteCommand(cmd);
        onFlushCommand();
    }
}
