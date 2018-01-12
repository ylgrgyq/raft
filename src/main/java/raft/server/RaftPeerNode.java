package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/1/12
 */
public class RaftPeerNode {
    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private int matchIndex;

    RaftPeerNode(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 1;
    }
}
