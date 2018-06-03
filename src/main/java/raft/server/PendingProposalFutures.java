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
    private final TreeMap<Integer, CompletableFuture<ProposalResponse>> pendingProposal = new TreeMap<>();

    void addFuture(int lastIndex, CompletableFuture<ProposalResponse> responseFuture) {
        assert lastIndex > 0;
        assert responseFuture != null;

        pendingProposal.put(lastIndex, responseFuture);
    }

    void completeFutures(int commitIndex) {
        NavigableMap<Integer, CompletableFuture<ProposalResponse>> fs = pendingProposal.headMap(commitIndex, true);

        for (Map.Entry<Integer, CompletableFuture<ProposalResponse>> entry : fs.entrySet()) {
            CompletableFuture<ProposalResponse> future = entry.getValue();
            future.complete(ProposalResponse.success());
            pendingProposal.remove(entry.getKey());
        }
    }
}
