package raft.server.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
public abstract class RaftServerCommand implements SerializableCommand {
    private int term;
    private CommandCode code;

    RaftServerCommand(){
    }

    RaftServerCommand(int term, CommandCode code) {
        this.code = code;
        this.term = term;
    }

    CommandCode getCommandCode() {
        return this.code;
    }

    void setCode(CommandCode code) {
        this.code = code;
    }

    public int getTerm() {
        return this.term;
    }

    @Override
    public byte[] encode() {
        final ByteBuffer buf = ByteBuffer.allocate(4);
        buf.putInt(this.term);
        return buf.array();
    }

    @Override
    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        this.term = buf.getInt();
        return buf;
    }
}
