package raft.server;

public interface TimeoutListener {
    void onTimeout(boolean electionTimeout, boolean pingTimeout);
}
