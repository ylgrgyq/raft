package raft.server;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public class RaftStatusSnapshot {
    public static final RaftStatusSnapshot emptyStatus = new RaftStatusSnapshot();

    private long term;
    private long commitIndex;
    private long appliedIndex;
    private String leaderId;
    private State state;
    private List<String> peerNodeIds;

    public RaftStatusSnapshot() {
        this.term = 0L;
        this.commitIndex = -1L;
        this.appliedIndex = -1L;
        this.leaderId = null;
        this.state = State.FOLLOWER;
        this.peerNodeIds = Collections.emptyList();
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public void setCommitIndex(long commitIndex) {
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

    public long getTerm() {
        return term;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public State getState() {
        return state;
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(long appliedIndex) {
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
                ", commitIndex=" + commitIndex +
                ", appliedIndex=" + appliedIndex +
                ", leaderId='" + leaderId + '\'' +
                ", state=" + state +
                ", peerNodeIds=" + peerNodeIds +
                '}';
    }
}
