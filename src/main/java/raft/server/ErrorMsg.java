package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public enum ErrorMsg {
    INTERNAL_ERROR(1, "Internal error"),
    NOT_LEADER_NODE(0, "This node is not leader"),
    EXISTS_UNAPPLIED_CONFIGURATION(2, "There's a pending unapplied configuration");


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
