package raft.server.rpc;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class RemotingCommand {
    private static final AtomicInteger requestIdGenerator = new AtomicInteger();

    private int requestId = requestIdGenerator.incrementAndGet();

    private CommandCode commandCode;
    private RemotingCommandType type;
    private byte[] body;

    private RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand(RaftServerCommand req) {
        RemotingCommand wrap = new RemotingCommand();
        wrap.setType(RemotingCommandType.REQUEST);
        wrap.setCommandCode(req.getCommandCode());
        wrap.setBody(req.encode());
        return wrap;
    }

    public static RemotingCommand createResponseCommand(RaftServerCommand res) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setType(RemotingCommandType.RESPONSE);
        cmd.setCommandCode(res.getCommandCode());
        cmd.setBody(res.encode());
        return cmd;
    }

    public ByteBuf encode(ByteBuf buf){
        int length = this.getLength();
        buf.writeInt(length);
        buf.writeByte(commandCode.getCode());
        buf.writeByte(type.getTypeCode());

        return buf;
    }

    public static RemotingCommand decode(ByteBuf buf) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.commandCode = CommandCode.valueOf(buf.readByte());
        cmd.type = RemotingCommandType.valueOf(buf.readByte());

        int length = buf.readableBytes();
        cmd.body = new byte[length];
        buf.readBytes(cmd.body);

        return cmd;
    }

    public int getLength(){
        return body.length + 4 + 2;
    }

    public CommandCode getCommandCode() {
        return commandCode;
    }

    public RemotingCommandType getType() {
        return type;
    }

    public byte[] getBody() {
        return body;
    }

    public void setCommandCode(CommandCode commandCode) {
        this.commandCode = commandCode;
    }

    public void setType(RemotingCommandType type) {
        this.type = type;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public int getRequestId() {
        return requestId;
    }

    public void setRequestId(int requestId) {
        this.requestId = requestId;
    }

    @Override
    public String toString() {
        return "RemotingCommand{" +
                "commandCode=" + commandCode +
                ", type=" + type +
                ", body=" + Arrays.toString(body) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemotingCommand)) return false;

        RemotingCommand that = (RemotingCommand) o;

        if (getCommandCode() != that.getCommandCode()) return false;
        if (getType() != that.getType()) return false;
        return Arrays.equals(getBody(), that.getBody());
    }

    @Override
    public int hashCode() {
        int result = getRequestId();
        result = 31 * result + getCommandCode().hashCode();
        result = 31 * result + getType().hashCode();
        result = 31 * result + Arrays.hashCode(getBody());
        return result;
    }
}
