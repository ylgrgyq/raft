package raft.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.LocalFilePersistentMeta;
import raft.server.util.ThreadFactoryImpl;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static raft.server.util.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLogImpl implements RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLogImpl.class.getName());
    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryImpl("RaftLogAsyncAppender-");

    private final ExecutorService pool;
    private final PersistentStorage storage;

    private CountDownLatch shutdownLatch;
    private LogsBuffer buffer;

    private long commitIndex;
    private long appliedIndex;

    private long recentSnapshotIndex;
    private long recentSnapshotTerm;

    public RaftLogImpl(PersistentStorage storage) {
        this.storage = storage;
        this.pool = Executors.newSingleThreadExecutor(defaultThreadFactory);
    }

    @Override
    public void init(LocalFilePersistentMeta meta) {
        storage.init();

        long lastIndex = storage.getLastIndex();
        if (lastIndex < 0L) {
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

        long firstIndex = storage.getFirstIndex();
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

    public synchronized long getFirstIndex() {
        if (recentSnapshotIndex > 0) {
            return recentSnapshotIndex;
        }

        return storage.getFirstIndex();
    }

    public synchronized long getLastIndex() {
        // buffer always know the last index
        return Math.max(buffer.getLastIndex(), recentSnapshotIndex);
    }

    public synchronized Optional<Long> getTerm(long index) {
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

    public Optional<LogEntry> getEntry(long index) {
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public synchronized List<LogEntry> getEntries(long start, long end) {
        checkArgument(start <= end, "invalid start and end: %s %s", start, end);

        if (start < getFirstIndex()) {
            throw new LogsCompactedException(start);
        }

        long lastIndex = getLastIndex();
        if (start == end || start > lastIndex) {
            return Collections.emptyList();
        }

        List<LogEntry> entries = new ArrayList<>();

        long bufferOffset = buffer.getOffsetIndex();
        if (start < bufferOffset) {
            entries.addAll(storage.getEntries(start, Math.min(end, bufferOffset)));
        }

        if (end > bufferOffset) {
            entries.addAll(buffer.getEntries(Math.max(start, bufferOffset), Math.min(end, buffer.getLastIndex() + 1)));
        }

        return entries;
    }

    @Override
    public synchronized CompletableFuture<Long> leaderAsyncAppend(List<LogEntry> entries) {
        // add logs to buffer first so we can read these new entries immediately during broadcasting logs to
        // followers afterwards and don't need to wait them to persistent in storage
        buffer.append(entries);
        return CompletableFuture.supplyAsync(() -> storage.append(entries), pool);
    }

    @Override
    public synchronized CompletableFuture<Long> followerAsyncAppend(long prevIndex, long prevTerm, List<LogEntry> entries) {
        if (match(prevTerm, prevIndex)) {
            long conflictIndex = searchConflict(entries);
            long lastIndex = prevIndex + entries.size();
            if (conflictIndex != 0) {
                assert conflictIndex - prevIndex - 1 < entries.size() :
                        "conflictIndex:" + conflictIndex + " prevIndex:" + prevIndex + " entriesSize:" + entries.size();

                if (conflictIndex <= commitIndex) {
                    logger.error("try append entries conflict with committed entry on index: {}, " +
                                    "new entry: {}, committed entry: {}",
                            conflictIndex, entries.get((int) (conflictIndex - prevIndex - 1)), getEntry(conflictIndex));
                    throw new IllegalStateException();
                }

                List<LogEntry> entriesNeedToStore = entries.subList((int) (conflictIndex - prevIndex - 1), entries.size());
                buffer.append(entriesNeedToStore);
                return CompletableFuture.supplyAsync(() -> storage.append(entriesNeedToStore), pool);
            }

            return CompletableFuture.completedFuture(lastIndex);
        }

        return CompletableFuture.completedFuture(-1L);
    }

    private long searchConflict(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (!match(entry.getTerm(), entry.getIndex())) {
                if (entry.getIndex() <= getLastIndex()) {
                    logger.warn("found conflict entry at index {}, existing term: {}, conflicting term: {}",
                            entry.getIndex(), getTerm(entry.getIndex()).orElse(0L), entry.getTerm());
                }
                return entry.getIndex();
            }
        }

        return 0;
    }

    public boolean match(long term, long index) {
        Optional<Long> storedTerm = getTerm(index);

        return storedTerm.isPresent() && term == storedTerm.get();
    }

    public synchronized boolean isUpToDate(long term, long index) {
        long lastIndex = getLastIndex();
        Optional<Long> lastTerm = getTerm(lastIndex);

        assert lastTerm.isPresent();

        return term > lastTerm.get() ||
                (term == lastTerm.get() &&
                        index >= lastIndex);
    }

    public synchronized long getCommitIndex() {
        return commitIndex;
    }

    public synchronized long getAppliedIndex() {
        return appliedIndex;
    }

    public synchronized List<LogEntry> tryCommitTo(long commitTo) {
        checkArgument(commitTo <= getLastIndex(),
                "try commit to %s but last index in log is %s", commitTo, getLastIndex());
        long oldCommit = getCommitIndex();
        if (commitTo > oldCommit) {
            commitIndex = commitTo;
            return getEntries(oldCommit + 1, commitTo + 1);
        }

        return Collections.emptyList();
    }

    public synchronized void appliedTo(long appliedTo) {
        long commitIndex = this.commitIndex;
        long appliedIndex = this.appliedIndex;
        checkArgument(appliedTo <= this.commitIndex,
                "try applied log to %s but commit index in log is %s", appliedTo, commitIndex);
        checkArgument(appliedTo >= appliedIndex,
                "try applied log to %s but applied index in log is %s", appliedTo, appliedIndex);
        this.appliedIndex = appliedTo;
        buffer.truncateBuffer(Math.min(appliedTo, storage.getLastIndex()));
    }

    @Override
    public synchronized void installSnapshot(LogSnapshot snapshot) {
        logger.info("receive snapshot with index:{} and term: {}", snapshot.getIndex(), snapshot.getTerm());

        commitIndex = snapshot.getIndex();

        recentSnapshotIndex = snapshot.getIndex();
        recentSnapshotTerm = snapshot.getTerm();

        buffer.installSnapshot(snapshot);
    }

    @Override
    public synchronized void snapshotApplied(long snapshotIndex) {
        if (recentSnapshotIndex == snapshotIndex) {
            recentSnapshotIndex = -1;
            recentSnapshotTerm = -1;
        }
    }

    @Override
    public synchronized void shutdown() {
        if (shutdownLatch != null) {
            return;
        }

        shutdownLatch = new CountDownLatch(1);
        pool.submit(() -> {
            synchronized (this) {
                try {
                    pool.shutdown();
                    storage.awaitTermination();
                } catch (InterruptedException ex) {
                    // ignore
                } finally {
                    shutdownLatch.countDown();
                }
            }
        });
    }

    @Override
    public void awaitTermination() throws InterruptedException {
        if (shutdownLatch == null) {
            shutdown();
        }

        shutdownLatch.await();
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
