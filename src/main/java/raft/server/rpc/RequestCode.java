package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/1
 */
public enum RequestCode {
    REQUEST_VOTE(1),
    APPEND_ENTRIES(2);

    private int code;
    RequestCode(int code){
        this.code = code;
    }

    static RequestCode getRequestCode(int code) {
        if (code == 1) {
            return RequestCode.REQUEST_VOTE;
        } else {
            return RequestCode.APPEND_ENTRIES;
        }
    }
}
