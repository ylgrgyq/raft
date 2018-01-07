package raft.server.rpc;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public class RaftClientCommand extends RaftCommand {
    private byte[] requestBody;
    private String leaderId = "";
    private boolean success = false;

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

        int leaderIdLength = this.leaderId.length();
        buffer.putInt(leaderIdLength);
        buffer.put(this.leaderId.getBytes(StandardCharsets.UTF_8));

        buffer.put(this.success ? (byte) 1 : (byte) 0);

        return buffer.array();
    }

    @Override
    public ByteBuffer decode(byte[] bytes) {
        final ByteBuffer buf = super.decode(bytes);

        int bodyLength = buf.getInt();
        requestBody = new byte[bodyLength];
        buf.get(this.requestBody);

        int leaderIdLength = buf.getInt();
        byte[] leaderId = new byte[leaderIdLength];
        buf.get(leaderId);
        this.leaderId = new String(leaderId);

        this.success = buf.get() == 1;
        return buf;
    }

    @Override
    public String toString() {
        return "RaftClientCommand{" +
                "requestBodyLength=" + this.requestBody.length +
                ", leaderId='" + leaderId + '\'' +
                ", success=" + success +
                '}';
    }

    public byte[] getRequestBody() {
        return requestBody;
    }

    public void setRequestBody(byte[] requestBody) {
        this.requestBody = requestBody;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }
}
