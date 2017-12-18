package raft.server.processor;

import raft.server.rpc.RaftServerCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/18
 */
public interface RaftServerCommandListener<T extends RaftServerCommand> {
    void onReceiveRaftServerCommand(T cmd);
}
