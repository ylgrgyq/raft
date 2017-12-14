package raft.server.state;

import raft.server.LifeCycle;
import raft.server.rpc.AppendEntriesCommand;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public abstract class RaftState implements LifeCycle {
    public void onReceiveAppendEntries(AppendEntriesCommand cmd){}
}
