package raft.server.log;

import raft.server.LocalFilePersistentMeta;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Author: ylgrgyq
 * Date: 18/5/16
 */
public interface RaftLog {
    void init(LocalFilePersistentMeta meta);

    long getLastIndex();

    long getFirstIndex();

    Optional<Long> getTerm(long index);

    Optional<LogEntry> getEntry(long index);

    List<LogEntry> getEntries(long start, long end);

    boolean match(long term, long index);

    CompletableFuture<Long> leaderAsyncAppend(List<LogEntry> entries);

    CompletableFuture<Long> followerAsyncAppend(long prevIndex, long prevTerm, List<LogEntry> entries);

    boolean isUpToDate(long term, long index);

    long getCommitIndex();

    long getAppliedIndex();

    List<LogEntry> tryCommitTo(long commitTo);

    void appliedTo(long appliedTo);

    void installSnapshot(LogSnapshot snapshot);

    void snapshotApplied(long snapshotIndex);

    /**
     * Shutdown RaftLog service. Any write operation after this call will not guarantee to execute.
     * When the call returned, please use awaitTermination() to wait the RaftLog service actually terminated.
     */
    void shutdown();

    void awaitTermination() throws InterruptedException;
}
