package raft.server.rpc;

import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Author: ylgrgyq
 * Date: 17/12/4
 */
public class PendingRequest {
    private org.slf4j.Logger logger = LoggerFactory.getLogger(PendingRequest.class.getName());

    private PendingRequestCallback callback;
    private RemotingCommand req;
    private RemotingCommand res;
    private AtomicBoolean responseAlreadySet = new AtomicBoolean(false);

    public PendingRequest(RemotingCommand request) {
        this(request, null);
    }

    public PendingRequest(RemotingCommand request, PendingRequestCallback callback) {
        this.req = request;
        this.callback = callback;
    }

    public void executeCallback() throws Exception {
        if (callback != null) {
            callback.operationComplete(this);
        }
    }

    public PendingRequestCallback getCallback() {
        return callback;
    }

    public RemotingCommand getRequest() {
        return req;
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
                ", req=" + req.toString() +
                ", res=" + res.toString() +
                '}';
    }
}
