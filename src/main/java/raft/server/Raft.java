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

	void receiveCommand(RaftCommand cmd);

	RaftStatusSnapshot getStatus();

	String getId();

	boolean isLeader();

	State getState();

	void start();

	void shutdown();
}
