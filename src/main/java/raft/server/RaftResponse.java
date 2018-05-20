package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/28
 */
public class RaftResponse {
    private String leaderIdHint;
    private ErrorMsg error;

    public RaftResponse(String leaderIdHint) {
        this.leaderIdHint = leaderIdHint;
    }

    public RaftResponse(String leaderIdHint, ErrorMsg error) {
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
