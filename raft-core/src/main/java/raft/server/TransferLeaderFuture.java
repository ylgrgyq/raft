package raft.server;

import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq
 * Date: 18/6/3
 */
class TransferLeaderFuture {
    private final CompletableFuture<ProposalResponse> responseFuture;
    private final String transfereeId;

    TransferLeaderFuture(String transfereeId, CompletableFuture<ProposalResponse> future) {
        assert transfereeId != null;
        assert future != null;

        this.responseFuture = future;
        this.transfereeId = transfereeId;
    }

    CompletableFuture<ProposalResponse> getResponseFuture() {
        return responseFuture;
    }

    String getTransfereeId() {
        return transfereeId;
    }
}
