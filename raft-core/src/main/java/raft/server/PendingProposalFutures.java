package raft.server;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq
 * Date: 18/6/3
 */
class PendingProposalFutures {
    private final TreeMap<Long, CompletableFuture<ProposalResponse>> pendingProposal = new TreeMap<>();

    void addFuture(long lastIndex, CompletableFuture<ProposalResponse> responseFuture) {
        if (responseFuture != Proposal.voidFuture) {
            assert lastIndex > 0;
            assert responseFuture != null;

            pendingProposal.put(lastIndex, responseFuture);
        }
    }

    void completeFutures(long commitIndex) {
        NavigableMap<Long, CompletableFuture<ProposalResponse>> fs = pendingProposal.headMap(commitIndex, true);

        for (Map.Entry<Long, CompletableFuture<ProposalResponse>> entry : fs.entrySet()) {
            CompletableFuture<ProposalResponse> future = entry.getValue();
            future.complete(ProposalResponse.success());
            pendingProposal.remove(entry.getKey());
        }
    }

    void failedAllFutures() {
        for (Map.Entry<Long, CompletableFuture<ProposalResponse>> entry : pendingProposal.entrySet()) {
            CompletableFuture<ProposalResponse> future = entry.getValue();
            future.complete(ProposalResponse.error(ErrorMsg.NOT_LEADER));
            pendingProposal.remove(entry.getKey());
        }
    }
}
