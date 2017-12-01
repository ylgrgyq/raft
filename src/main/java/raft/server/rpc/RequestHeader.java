package raft.server.rpc;

import io.netty.buffer.ByteBuf;

/**
 * Author: ylgrgyq
 * Date: 17/12/1
 */
public class RequestHeader {
    private RequestCode requestCode;
    private int term;

    public static RequestHeader decode(ByteBuf buf) {
        RequestHeader header = new RequestHeader();

        header.requestCode = RequestCode.getRequestCode(buf.readInt());
        header.term = buf.readInt();

        return header;
    }

    public RequestCode getRequestCode() {
        return requestCode;
    }

    public int getTerm() {
        return term;
    }

    public void setRequestCode(RequestCode requestCode) {
        this.requestCode = requestCode;
    }

    public void setTerm(int term) {
        this.term = term;
    }
}
