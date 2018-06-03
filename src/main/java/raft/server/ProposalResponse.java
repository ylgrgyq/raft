package raft.server;

import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/3/28
 */
public class ProposalResponse {
    private final String leaderIdHint;
    private final ErrorMsg error;

    private ProposalResponse() {
        this("", ErrorMsg.NONE);
    }

    private ProposalResponse(String leaderIdHint, ErrorMsg error) {
        this.leaderIdHint = leaderIdHint;
        this.error = error;
    }

    public static ProposalResponse success(){
        return new ProposalResponse();
    }

    public static ProposalResponse error(ErrorMsg error){
        return new ProposalResponse(null, error);
    }

    public static ProposalResponse errorWithLeaderHint(String leaderIdHint, ErrorMsg error){
        return new ProposalResponse(leaderIdHint, error);
    }

    public Optional<String> getLeaderIdHint() {
        return Optional.ofNullable(leaderIdHint);
    }

    public boolean isSuccess() {
        return error == ErrorMsg.NONE;
    }

    public ErrorMsg getError() {
        return error;
    }
}
