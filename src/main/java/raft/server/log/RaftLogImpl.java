package raft.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.RaftPersistentMeta;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.BiConsumer;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLogImpl implements RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLogImpl.class.getName());
    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryImpl("RaftLogAsyncAppender-");

    private final ExecutorService pool;
    private final PersistentStorage storage;

    private LogsBuffer buffer;

    private int commitIndex;
    private int appliedIndex;

    private int recentSnapshotIndex;
    private int recentSnapshotTerm;

    public RaftLogImpl(PersistentStorage storage) {
        this.storage = storage;
        this.pool = Executors.newSingleThreadExecutor(defaultThreadFactory);
    }

    @Override
    public void init(RaftPersistentMeta meta) {
        storage.init();

        int lastIndex = storage.getLastIndex();
        if (lastIndex < 0) {
            storage.append(Collections.singletonList(PersistentStorage.sentinel));
            lastIndex = storage.getLastIndex();
        }

        List<LogEntry> entries = storage.getEntries(lastIndex, lastIndex + 1);
        if (entries.isEmpty()) {
            String msg = String.format("failed to get LogEntry from persistent storage with " +
                    "storage last index: %s", lastIndex);
            throw new IllegalStateException(msg);
        }

        buffer = new LogsBuffer(entries.get(0));

        int firstIndex = storage.getFirstIndex();
        // we shell assume every logs in storage is not committed and applied including the first dummy empty log and
        // initialize commitIndex and appliedIndex to the index before the firstIndex in storage. Because we don't know where
        // the log has already committed or applied to at this moment. If we have persistent commitIndex and
        // appliedIndex, we will restore it latter
        this.commitIndex = firstIndex - 1;
        this.appliedIndex = firstIndex - 1;

        if (meta.getCommitIndex() > -1) {
            this.commitIndex = meta.getCommitIndex();
        }
    }

    public synchronized int getFirstIndex() {
        if (recentSnapshotIndex > 0) {
            return recentSnapshotIndex;
        }

        return storage.getFirstIndex();
    }

    public synchronized int getLastIndex() {
        // buffer always know the last index
        return Math.max(buffer.getLastIndex(), recentSnapshotIndex);
    }

    public synchronized Optional<Integer> getTerm(int index){
        if (index < getFirstIndex()) {
            throw new LogsCompactedException(index);
        }

        if (index > getLastIndex()) {
            return Optional.empty();
        }

        if (index >= buffer.getOffsetIndex()) {
            return Optional.of(buffer.getTerm(index));
        }

        if (recentSnapshotIndex > 0 && index == recentSnapshotIndex) {
            return Optional.of(recentSnapshotTerm);
        }

        return Optional.of(storage.getTerm(index));
    }

    public Optional<LogEntry> getEntry(int index){
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public synchronized List<LogEntry> getEntries(int start, int end){
        checkArgument(start <= end, "invalid start and end: %s %s", start, end);

        if (start < getFirstIndex()) {
            throw new LogsCompactedException(start);
        }

        int lastIndex = getLastIndex();
        if (start == end || start > lastIndex) {
            return Collections.emptyList();
        }

        List<LogEntry> entries = new ArrayList<>();

        int bufferOffset = buffer.getOffsetIndex();
        if (start < bufferOffset) {
            entries.addAll(storage.getEntries(start, Math.min(end, bufferOffset)));
        }

        if (end > bufferOffset) {
            entries.addAll(buffer.getEntries(Math.max(start, bufferOffset), Math.min(end, buffer.getLastIndex() + 1)));
        }

        return entries;
    }

    @Override
    public int leaderAsyncAppend(int term, List<LogEntry> entries, BiConsumer<? super Integer, ? super Throwable> callback) {
        if (entries.size() == 0) {
            return getLastIndex();
        }

        final ArrayList<LogEntry> preparedEntries = new ArrayList<>(entries.size());
        int i = getLastIndex();
        for (LogEntry entry : entries) {
            ++i;
            LogEntry e = LogEntry.newBuilder(entry)
                    .setIndex(i)
                    .setTerm(term)
                    .build();
            preparedEntries.add(e);
        }

        synchronized(this) {
            // add logs to buffer first so we can read these new entries immediately during broadcasting logs to
            // followers afterwards and don't need to wait them to persistent in storage
            buffer.append(preparedEntries);
            CompletableFuture.supplyAsync(() -> {
                storage.append(preparedEntries);
                return getLastIndex();
            }, pool).whenComplete(callback);
        }

        return i;
    }

    @Override
    public synchronized int followerSyncAppend(int prevIndex, int prevTerm, List<LogEntry> entries) {
        if (match(prevTerm, prevIndex)) {
            int conflictIndex = searchConflict(entries);
            int lastIndex = prevIndex + entries.size();
            if (conflictIndex != 0) {
                if (conflictIndex <= commitIndex) {
                    logger.error("try append entries conflict with committed entry on index: {}, " +
                                    "new entry: {}, committed entry: {}",
                            conflictIndex, entries.get(conflictIndex - prevIndex - 1), getEntry(conflictIndex));
                    throw new IllegalStateException();
                }

                List<LogEntry> entriesNeedToStore = entries.subList(conflictIndex - prevIndex - 1, entries.size());
                buffer.append(entriesNeedToStore);
                storage.append(entriesNeedToStore);
            }
            return lastIndex;
        }

        return -1;
    }

    private int searchConflict(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (!match(entry.getTerm(), entry.getIndex())) {
                if (entry.getIndex() <= getLastIndex()) {
                    logger.warn("found conflict entry at index {}, existing term: {}, conflicting term: {}",
                            entry.getIndex(), getTerm(entry.getIndex()), entry.getTerm());
                }
                return entry.getIndex();
            }
        }

        return 0;
    }

    public boolean match(int term, int index) {
        Optional<Integer> storedTerm = getTerm(index);

        return storedTerm.isPresent() && term == storedTerm.get();
    }

    public synchronized boolean isUpToDate(int term, int index) {
        int lastIndex = getLastIndex();
        Optional<Integer> lastTerm = getTerm(lastIndex);

        assert lastTerm.isPresent();

        return term > lastTerm.get() ||
                (term == lastTerm.get() &&
                        index >= lastIndex);
    }

    public synchronized int getCommitIndex() {
        return commitIndex;
    }

    public synchronized int getAppliedIndex() {
        return appliedIndex;
    }

    public synchronized List<LogEntry> tryCommitTo(int commitTo) {
        checkArgument(commitTo <= getLastIndex(),
                "try commit to %s but last index in log is %s", commitTo, getLastIndex());
        int oldCommit = getCommitIndex();
        if (commitTo > oldCommit) {
            commitIndex = commitTo;
            return getEntries(oldCommit + 1, commitTo + 1);
        }

        return Collections.emptyList();
    }

    public synchronized void appliedTo(int appliedTo) {
        int commitIndex = this.commitIndex;
        int appliedIndex = this.appliedIndex;
        checkArgument(appliedTo <= this.commitIndex,
                "try applied log to %s but commit index in log is %s", appliedTo, commitIndex);
        checkArgument(appliedTo >= appliedIndex,
                "try applied log to %s but applied index in log is %s", appliedTo, appliedIndex);

        this.appliedIndex = appliedTo;
        buffer.truncateBuffer(appliedTo);
    }

    @Override
    public synchronized void installSnapshot(LogSnapshot snapshot) {
        commitIndex = snapshot.getIndex();

        recentSnapshotIndex = snapshot.getIndex();
        recentSnapshotTerm = snapshot.getTerm();
    }

    @Override
    public synchronized void snapshotApplied(int snapshotIndex) {
        if (recentSnapshotIndex == snapshotIndex) {
            recentSnapshotIndex = -1;
            recentSnapshotTerm = -1;
        }
    }

    @Override
    public void shutdown() {
        pool.shutdown();
        storage.shutdown();
    }

    @Override
    public synchronized String toString() {
        return "{" +
                "commitIndex=" + commitIndex +
                ", appliedIndex=" + appliedIndex +
                ", firstIndex=" + getFirstIndex() +
                ", lastIndex=" + getLastIndex() +
                '}';
    }
}
