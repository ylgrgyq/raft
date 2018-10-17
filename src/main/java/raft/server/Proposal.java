package raft.server;

import com.google.protobuf.ByteString;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 18/6/3
 */
class Proposal {
    public static final CompletableFuture<ProposalResponse> voidFuture = CompletableFuture.completedFuture(null);

    private final List<ByteString> datas;
    private final LogEntry.EntryType type;
    private final CompletableFuture<ProposalResponse> future;

    Proposal(List<byte[]> entries, LogEntry.EntryType type) {
        this.datas = entries.stream().map(ByteString::copyFrom).collect(Collectors.toList());
        this.type = type;
        this.future = new CompletableFuture<>();
    }

    List<ByteString> getDatas() {
        return datas;
    }

    LogEntry.EntryType getType() {
        return type;
    }

    CompletableFuture<ProposalResponse> getFuture() {
        return future;
    }

    @Override
    public String toString() {
        return type + " Proposal with " + datas.size() + " datas";
    }
}
