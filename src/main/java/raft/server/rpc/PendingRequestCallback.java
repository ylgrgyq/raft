package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/5
 */
public interface PendingRequestCallback {
    void operationComplete(PendingRequest request) throws Exception;
}
