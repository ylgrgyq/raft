package raft.server;

public enum  RaftJobType {
    PROPOSAL,
    RAFT_COMMAND,
    TICK,
    UPDATE_COMMIT;
}
