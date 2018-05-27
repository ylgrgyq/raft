package raft.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/5
 */
public interface PendingRequestCallback {
    void operationComplete(PendingRequest request, RemotingCommand response) throws Exception;
}
