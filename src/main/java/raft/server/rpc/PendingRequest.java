package raft.server.rpc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: ylgrgyq
 * Date: 17/12/4
 */
public class PendingRequest {
    private Logger logger = LoggerFactory.getLogger(PendingRequest.class.getName());

    private final long requestBeginTimestamp = System.currentTimeMillis();
    private final long timeoutMillis;
    private final PendingRequestCallback callback;
    private final AtomicBoolean responseAlreadySet = new AtomicBoolean(false);

    private RemotingCommand res;

    public PendingRequest(long timeoutMillis) {
        this(timeoutMillis, null);
    }

    public PendingRequest(long timeoutMillis, PendingRequestCallback callback) {
        this.callback = callback;
        this.timeoutMillis = timeoutMillis;
    }

    public void executeCallback() throws Exception {
        if (callback != null) {
            callback.operationComplete(this);
        }
    }

    public boolean isTimeout() {
        return (requestBeginTimestamp + timeoutMillis) < System.currentTimeMillis();
    }

    public PendingRequestCallback getCallback() {
        return callback;
    }

    public RemotingCommand getResponse() {
        return res;
    }

    public void setResponse(RemotingCommand res) {
        if (res != null) {
            if (responseAlreadySet.compareAndSet(false, true)) {
                this.res = res;
            } else {
                logger.warn("duplicate set response for pending request");
                logger.warn(this.toString());
            }
        }
    }

    @Override
    public String toString() {
        return "PendingRequest{" +
                ", res=" + res.toString() +
                '}';
    }
}
