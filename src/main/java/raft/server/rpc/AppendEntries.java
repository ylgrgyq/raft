package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntries implements Request{
    private long term;
    private String leaderId;
    private long prevLogIndex;
    private long prevLogTerm;

}
