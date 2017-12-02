package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public enum RemotingCommandType {
    REQUEST((byte) 1),
    RESPONSE((byte) 2);

    private byte type;

    RemotingCommandType(byte type) {
        this.type = type;
    }

    public static RemotingCommandType valueOf(byte value) {
        for (RemotingCommandType t : RemotingCommandType.values()) {
            if (t.type == value) {
                return t;
            }
        }

        return null;
    }

    byte getTypeCode(){
        return this.type;
    }
}
