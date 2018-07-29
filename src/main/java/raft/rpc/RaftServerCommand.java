package raft.rpc;

import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/12/7
 */
public abstract class RaftServerCommand extends RaftCommand {
    private int term;
    private String from;

    RaftServerCommand(){
    }

    RaftServerCommand(int term, String from, CommandCode code) {
        super(code);
        Preconditions.checkArgument(from != null && !from.isEmpty(),
                "need non empty from for RaftServerCommand");
        this.term = term;
        this.from = from;
    }

    public int getTerm() {
        return this.term;
    }

    public String getFrom() {
        return this.from;
    }

    @Override
    byte[] encode() {
        assert this.from != null;
        byte[] fromBytes = this.from.getBytes(StandardCharsets.UTF_8);

        final ByteBuffer buf = ByteBuffer.allocate(
                // term
                Integer.BYTES +
                // from
                Integer.BYTES + fromBytes.length);

        buf.putInt(this.term);
        buf.putInt(fromBytes.length);
        buf.put(fromBytes);

        return buf.array();
    }

    @Override
    ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = ByteBuffer.wrap(bytes);
        this.term = buf.getInt();

        int length = buf.getInt();
        assert length != 0 : "from in RaftServerCommand must not empty";

        byte[] leaderIdBytes = new byte[length];
        buf.get(leaderIdBytes);
        this.from = new String(leaderIdBytes, StandardCharsets.UTF_8);

        return buf;
    }
}
