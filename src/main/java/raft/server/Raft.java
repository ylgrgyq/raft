package raft.server;

import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq Date: 18/3/30
 */
public interface Raft {
	String getId();

    CompletableFuture<ProposalResponse> propose(List<byte[]> data);

	CompletableFuture<ProposalResponse> transferLeader(String transfereeId);

	CompletableFuture<ProposalResponse> addNode(String newNode);

	CompletableFuture<ProposalResponse> removeNode(String newNode);

	void receiveCommand(RaftCommand cmd);

	Raft start();

	void shutdown();

	void awaitTermination() throws InterruptedException;
}
