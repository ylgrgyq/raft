package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/28
 */
public class ProposeResponse {
    private String leaderId;
    private ErrorMsg error;

    public ProposeResponse(String leaderId) {
        this.leaderId = leaderId;
    }

    public ProposeResponse(String leaderId, ErrorMsg error) {
        this.leaderId = leaderId;
        this.error = error;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public boolean isSuccess() {
        return error == null;
    }
}
