package raft.server.processor;

import raft.server.rpc.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/18
 */
public interface RaftCommandListener<T extends RaftCommand> {
    void onReceiveRaftCommand(T cmd);
}
