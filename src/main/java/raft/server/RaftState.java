package raft.server;

import raft.server.rpc.AppendEntriesCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public abstract class RaftState {
    public abstract void start();

    public abstract void finish();

    public void onReceiveAppendEntries(AppendEntriesCommand cmd){}
}
