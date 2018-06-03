package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public enum ErrorMsg {
    NONE(0, "success"),
    INTERNAL_ERROR(1, "Internal error"),
    NOT_LEADER(2, "This node is not leader"),
    EXISTS_UNAPPLIED_CONFIGURATION(3, "There's a pending unapplied configuration"),
    FORBID_REMOVE_LEADER(4, "Need transfer leadership first then remove previous leader node"),
    ALLREADY_LEADER(5, "Transferee is a leader already"),
    LEADER_TRANSFERRING(6, "Transferring leadership to another node"),
    UNKNOWN_TRANSFEREEID(7, "Transfer leadership to a unknown node");

    private int code;
    private String msg;

    ErrorMsg(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "{" +
                "code=" + code +
                ", msg='" + msg + '\'' +
                '}';
    }
}
