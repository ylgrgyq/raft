package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/28
 */
public class ProposeResponse {
    private String leaderIdHint;
    private ErrorMsg error;

    public ProposeResponse(String leaderIdHint) {
        this.leaderIdHint = leaderIdHint;
    }

    public ProposeResponse(String leaderIdHint, ErrorMsg error) {
        this.leaderIdHint = leaderIdHint;
        this.error = error;
    }

    public String getLeaderIdHint() {
        return leaderIdHint;
    }

    public boolean isSuccess() {
        return error == null;
    }

    public ErrorMsg getError() {
        return error;
    }
}
