package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    long commited;
    long applied;

    void append(int term, byte[] log) {

    }
}
