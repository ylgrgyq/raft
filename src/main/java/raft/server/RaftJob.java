package raft.server;

import raft.server.proto.RaftCommand;

public class RaftJob {
    private RaftJobType type;

    private Proposal proposal;
    private RaftCommand command;


    static RaftJob tick() {
        return new RaftJob(RaftJobType.TICK);
    }

    static RaftJob updateCommit() {
        return new RaftJob(RaftJobType.TICK);
    }

    public RaftJob(RaftJobType type) {
        this.type = type;
    }

    public RaftJob(Proposal proposal) {
        this.type = RaftJobType.PROPOSAL;
        this.proposal = proposal;
    }

    public RaftJob(RaftCommand command) {
        this.type = RaftJobType.RAFT_COMMAND;
        this.command = command;
    }

    public RaftJobType getType() {
        return type;
    }

    public Proposal getProposal() {
        return proposal;
    }

    public RaftCommand getCommand() {
        return command;
    }
}
