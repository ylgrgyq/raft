package raft.server.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public abstract class RaftCommand {
    static byte[] EMPTY_BYTES = new byte[0];

    private CommandCode code;

    RaftCommand(){
    }

    RaftCommand(CommandCode code) {
        this.code = code;
    }

    CommandCode getCommandCode() {
        return this.code;
    }

    void setCode(CommandCode code) {
        this.code = code;
    }

    byte[] encode() {
        return RaftCommand.EMPTY_BYTES;
    }

    ByteBuffer decode(byte[] bytes) {
        return ByteBuffer.wrap(bytes);
    }
}
