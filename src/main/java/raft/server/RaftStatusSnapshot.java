package raft.server;

import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public class RaftStatusSnapshot {
    private String id;
    private int term;
    private String votedFor;
    private int commitIndex;
    private int appliedIndex;
    private String leaderId;
    private State state;
    private List<String> peerNodeIds;

    public void setId(String id) {
        this.id = id;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setVotedFor(String votedFor) {
        this.votedFor = votedFor;
    }

    public void setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public void setState(State state) {
        this.state = state;
    }

    public void setPeerNodeIds(List<String> peerNodeIds) {
        this.peerNodeIds = peerNodeIds;
    }

    public String getId() {
        return id;
    }

    public int getTerm() {
        return term;
    }

    public String getVotedFor() {
        return votedFor;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public State getState() {
        return state;
    }

    public int getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(int appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    public List<String> getPeerNodeIds() {
        return peerNodeIds;
    }

    @Override
    public String toString() {
        return "{" +
                "id='" + id + '\'' +
                ", term=" + term +
                ", votedFor='" + votedFor + '\'' +
                ", commitIndex=" + commitIndex +
                ", appliedIndex=" + appliedIndex +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + state +
                ", peerNodeIds=" + peerNodeIds +
                '}';
    }
}
