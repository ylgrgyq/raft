package raft.server;

import raft.server.proto.RaftCommand;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq Date: 18/3/30
 */
public interface Raft {

	CompletableFuture<ProposalResponse> transferLeader(String transfereeId);

	CompletableFuture<ProposalResponse> propose(List<byte[]> data);

	CompletableFuture<ProposalResponse> addNode(String newNode);

	CompletableFuture<ProposalResponse> removeNode(String newNode);

	String getId();

	void receiveCommand(RaftCommand cmd);

	Raft start();

	void shutdown();
}
