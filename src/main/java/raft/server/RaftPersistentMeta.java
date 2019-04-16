package raft.server;

public interface RaftPersistentMeta {
    String getVotedFor();

    void setVotedFor(String votedFor);

    long getTerm();

    void setTerm(long term);

    void setTermAndVotedFor(long term, String votedFor);

    long getCommitIndex();

    void setCommitIndex(long commitIndex);
}
