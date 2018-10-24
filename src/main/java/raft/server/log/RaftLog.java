package raft.server.log;

import raft.server.RaftPersistentMeta;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Author: ylgrgyq
 * Date: 18/5/16
 */
public interface RaftLog {
    void init(RaftPersistentMeta meta);

    long getLastIndex();

    long getFirstIndex();

    Optional<Long> getTerm(long index);

    Optional<LogEntry> getEntry(long index);

    List<LogEntry> getEntries(long start, long end);

    boolean match(long term, long index);

    CompletableFuture<Long> leaderAsyncAppend(List<LogEntry> entries);

    long followerSyncAppend(long prevIndex, long prevTerm, List<LogEntry> entries);

    boolean isUpToDate(long term, long index);

    long getCommitIndex();

    long getAppliedIndex();

    List<LogEntry> tryCommitTo(long commitTo);

    void appliedTo(long appliedTo);

    void installSnapshot(LogSnapshot snapshot);

    void snapshotApplied(long snapshotIndex);

    void shutdownNow();
}
