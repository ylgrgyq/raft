package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/28
 */
public class AppendResponse {
    private String leaderId;
    private boolean success;

    public AppendResponse(String leaderId, boolean success) {
        this.leaderId = leaderId;
        this.success = success;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isSuccess() {
        return success;
    }
}
