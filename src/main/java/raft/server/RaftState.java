package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/11/21
 */
public abstract class RaftState {
    public abstract void start();

    public abstract void finish();

}
