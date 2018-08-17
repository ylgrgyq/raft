package raft.server;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public class RaftStatusSnapshot {
    public static final RaftStatusSnapshot emptyStatus = new RaftStatusSnapshot();

    private int term;
    private String votedFor;
    private int commitIndex;
    private int appliedIndex;
    private String leaderId;
    private State state;
    private List<String> peerNodeIds;

    public RaftStatusSnapshot() {
        this.term = 0;
        this.votedFor = null;
        this.commitIndex = -1;
        this.appliedIndex = -1;
        this.leaderId = null;
        this.state = State.FOLLOWER;
        this.peerNodeIds = Collections.emptyList();
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

    public boolean isLeader() {
        return getState() == State.LEADER;
    }

    public boolean isFollower() {
        return getState() == State.FOLLOWER;
    }

    public boolean isCandidate() {
        return getState() == State.CANDIDATE;
    }

    @Override
    public String toString() {
        return "{" +
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
