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

    private int term;
    private CommandCode commandCode;
    private RemotingCommandType type;
    private byte[] body;

    private RemotingCommand() {
    }

    public static RemotingCommand createRequestCommand() {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setType(RemotingCommandType.REQUEST);
        return cmd;
    }

    public static RemotingCommand createResponseCommand() {
        RemotingCommand cmd = new RemotingCommand();
        cmd.setType(RemotingCommandType.RESPONSE);
        return cmd;
    }

    public ByteBuf encode(ByteBuf buf){
        int length = this.getLength();
        buf.writeInt(length);
        buf.writeInt(this.term);
        buf.writeByte(commandCode.getCode());
        buf.writeByte(type.getTypeCode());

        return buf;
    }

    public static RemotingCommand decode(ByteBuf buf) {
        RemotingCommand cmd = new RemotingCommand();
        cmd.term = buf.readInt();
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

    public int getTerm() {
        return term;
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

    public void setTerm(int term) {
        this.term = term;
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
                "term=" + term +
                ", commandCode=" + commandCode +
                ", type=" + type +
                ", body=" + Arrays.toString(body) +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemotingCommand)) return false;

        RemotingCommand that = (RemotingCommand) o;

        if (getTerm() != that.getTerm()) return false;
        if (getCommandCode() != that.getCommandCode()) return false;
        if (getType() != that.getType()) return false;
        return Arrays.equals(getBody(), that.getBody());
    }

    @Override
    public int hashCode() {
        int result = getTerm();
        result = 31 * result + (getCommandCode() != null ? getCommandCode().hashCode() : 0);
        result = 31 * result + (getType() != null ? getType().hashCode() : 0);
        result = 31 * result + Arrays.hashCode(getBody());
        return result;
    }
}
