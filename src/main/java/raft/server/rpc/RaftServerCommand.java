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

    public CommandCode getCommandCode() {
        return this.code;
    }

    public int getTerm() {
        return this.term;
    }

    @Override
    public byte[] encode() {
        final ByteBuffer buf = ByteBuffer.allocate(5);
        buf.putInt(this.term);
        buf.put(code.getCode());
        return buf.array();
    }

    @Override
    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        this.term = buf.getInt();
        this.code = CommandCode.valueOf(buf.get());
        return buf;
    }
}
