package raft.server;

import raft.server.proto.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
interface RaftCommandProcessor {
    State getBindingState();

    void start();

    void process(RaftCommand cmd);

    void finish();

    default void onElectionTimeout() {}

    default void onPingTimeout() {}
}
