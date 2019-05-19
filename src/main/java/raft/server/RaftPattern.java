package raft.server;

import raft.server.proto.RaftCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
abstract class RaftPattern {
    abstract void start();

    abstract void process(RaftCommand cmd);

    public void onElectionTimeout() {}

    public void onPingTimeout() {}

    abstract void finish();
}
