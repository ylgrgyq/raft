package raft.server;

import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq
 * Date: 18/6/3
 */
public class Proposal {
    private final List<LogEntry> entries;
    private final LogEntry.EntryType type;
    private final CompletableFuture<ProposalResponse> future;

    Proposal(List<LogEntry> entries, LogEntry.EntryType type) {
        this.entries = entries;
        this.type = type;
        this.future = new CompletableFuture<>();
    }

    List<LogEntry> getEntries() {
        return entries;
    }

    public LogEntry.EntryType getType() {
        return type;
    }

    CompletableFuture<ProposalResponse> getFuture() {
        return future;
    }
}
