package raft.server.log;

import raft.server.RaftPersistentState;
import raft.server.proto.LogEntry;
import raft.server.proto.Snapshot;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

/**
 * Author: ylgrgyq
 * Date: 18/5/16
 */
public interface RaftLog {
    void init(RaftPersistentState meta);

    int getLastIndex();

    int getFirstIndex();

    Optional<Integer> getTerm(int index);

    Optional<LogEntry> getEntry(int index);

    List<LogEntry> getEntries(int start, int end);

    boolean match(int term, int index);

    int leaderAsyncAppend(int term, List<LogEntry> entries, BiConsumer<? super Integer, ? super Throwable> action);

    int followerSyncAppend(int prevIndex, int prevTerm, List<LogEntry> entries);

    boolean isUpToDate(int term, int index);

    int getCommitIndex();

    int getAppliedIndex();

    List<LogEntry> tryCommitTo(int commitTo);

    void appliedTo(int appliedTo);

    void installSnapshot(Snapshot snapshot);

    void snapshotApplied(int snapshotIndex);

    void shutdown();
}
