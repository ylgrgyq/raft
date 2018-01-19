package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/1
 */
public enum CommandCode {
    NONE((byte) 0),
    REQUEST_VOTE((byte) 1),
    APPEND_ENTRIES((byte) 2),
    CLIENT_REQUEST((byte) 3);

    private byte code;
    CommandCode(byte code){
        this.code = code;
    }

    static CommandCode valueOf(byte code) {
        for (CommandCode c : CommandCode.values()) {
            if (c.code == code) {
                return c;
            }
        }

        return null;
    }

    byte getCode(){
        return this.code;
    }
}
