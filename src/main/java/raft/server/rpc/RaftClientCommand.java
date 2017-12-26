package raft.server.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public class RaftClientCommand extends RaftCommand {
    private byte[] requestBody;

    public RaftClientCommand(byte[] body){
        this.setCode(CommandCode.CLIENT_REQUEST);
        this.decode(body);
    }

    public RaftClientCommand() {
        super(CommandCode.REQUEST_VOTE);
    }

    @Override
    public byte[] encode() {
        byte[] base = super.encode();

        byte[] body = requestBody != null ? requestBody : SerializableCommand.EMPTY_BYTES;
        ByteBuffer buffer = ByteBuffer.allocate(base.length + 4 + body.length);
        buffer.put(base);
        buffer.putInt(body.length);
        buffer.put(body);

        return buffer.array();
    }

    @Override
    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = super.decode(bytes);

        int bodyLength = buf.getInt();
        requestBody = new byte[bodyLength];
        buf.get(this.requestBody);
        return buf;
    }
}
