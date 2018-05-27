package raft.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

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

    public RaftLogImpl(PersistentStorage storage) {
        this.storage = storage;
        this.pool = Executors.newSingleThreadExecutor(defaultThreadFactory);
    }

    @Override
    public void init() {
        storage.init();

        int lastIndex = storage.getLastIndex();
        List<LogEntry> entries = storage.getEntries(lastIndex, lastIndex + 1);
        if (entries.isEmpty()) {
            String msg = String.format("failed to get LogEntry from persistent storage with " +
                    "storage last index: %s", lastIndex);
            throw new IllegalStateException(msg);
        }

        buffer = new LogsBuffer(entries.get(0));

        int firstIndex = storage.getFirstIndex();
        this.commitIndex = firstIndex - 1;
        this.appliedIndex = firstIndex - 1;
    }

    public int getLastIndex() {
        // buffer always know the last index
        return buffer.getLastIndex();
    }

    public Optional<Integer> getTerm(int index) {
        if (index < storage.getFirstIndex() || index > getLastIndex()) {
            return Optional.empty();
        }

        if (index >= buffer.getOffsetIndex()) {
            return Optional.of(buffer.getTerm(index));
        }

        return storage.getTerm(index);
    }

    public Optional<LogEntry> getEntry(int index) {
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public List<LogEntry> getEntries(int start, int end) {
        checkArgument(start <= end, "invalid start and end: %s %s", start, end);

        int firstIndex = storage.getFirstIndex();
        int lastIndex = getLastIndex();
        checkArgument(start >= firstIndex,
                "start index %s out of bound %s",
                start, firstIndex);

        checkArgument(end <= lastIndex,
                "end index %s out of bound %s",
                start, lastIndex);

        if (start == end) {
            return Collections.emptyList();
        }

        List<LogEntry> entries = new ArrayList<>();

        int bufferOffset = buffer.getOffsetIndex();
        if (start < bufferOffset) {
            entries.addAll(storage.getEntries(start, Math.min(end, bufferOffset)));
        }

        if (end > bufferOffset) {
            entries.addAll(buffer.getEntries(start, end));
        }

        return entries;
    }

    public synchronized CompletableFuture<Integer> leaderAsyncAppend(int term, List<LogEntry> entries) {
        if (entries.size() == 0) {
            return CompletableFuture.completedFuture(this.getLastIndex());
        }

        final ArrayList<LogEntry> preparedEntries = new ArrayList<>(entries.size());
        int i = this.getLastIndex();
        for (LogEntry entry : entries) {
            ++i;
            LogEntry e = LogEntry.newBuilder(entry)
                    .setIndex(i)
                    .setTerm(term)
                    .build();
            preparedEntries.add(e);
        }

        // add logs to buffer first so we can read these new entries immediately during broadcasting logs to
        // followers afterwards and don't need to wait them to persistent in storage
        buffer.append(preparedEntries);
        return CompletableFuture.supplyAsync(() -> {
            storage.append(preparedEntries);
            return this.getLastIndex();
        }, pool);
    }

    public synchronized int followerSyncAppend(int prevIndex, int prevTerm, List<LogEntry> entries) {
        if (this.match(prevTerm, prevIndex)) {
            int conflictIndex = searchConflict(entries);
            int lastIndex = prevIndex + entries.size();
            if (conflictIndex != 0) {
                if (conflictIndex <= this.commitIndex) {
                    logger.error("try append entries conflict with committed entry on index: {}, " +
                                    "new entry: {}, committed entry: {}",
                            conflictIndex, entries.get(conflictIndex - prevIndex - 1), this.getEntry(conflictIndex));
                    throw new IllegalStateException();
                }

                List<LogEntry> entriesNeedToStore = entries.subList(conflictIndex - prevIndex - 1, entries.size());
                buffer.append(entriesNeedToStore);
                storage.append(entriesNeedToStore);
            }
            return lastIndex;
        }

        return 0;
    }

    private int searchConflict(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (!this.match(entry.getTerm(), entry.getIndex())) {
                if (entry.getIndex() <= this.getLastIndex()) {
                    logger.warn("found conflict entry at index {}, existing term: {}, conflicting term: {}",
                            entry.getIndex(), this.getTerm(entry.getIndex()), entry.getTerm());
                }
                return entry.getIndex();
            }
        }

        return 0;
    }

    private boolean match(int term, int index) {
        Optional<Integer> storedTerm = this.getTerm(index);

        return storedTerm.isPresent() && term == storedTerm.get();
    }

    public synchronized boolean isUpToDate(int term, int index) {
        Optional<LogEntry> lastEntryOnServerOpt = this.getEntry(this.getLastIndex());
        assert lastEntryOnServerOpt.isPresent();

        LogEntry lastEntryOnServer = lastEntryOnServerOpt.get();
        return term > lastEntryOnServer.getTerm() ||
                (term == lastEntryOnServer.getTerm() &&
                        index >= lastEntryOnServer.getIndex());
    }

    public synchronized int getCommitIndex() {
        return this.commitIndex;
    }

    public synchronized int getAppliedIndex() {
        return this.appliedIndex;
    }

    public synchronized List<LogEntry> tryCommitTo(int commitTo) {
        checkArgument(commitTo <= this.getLastIndex(),
                "try commit to %s but last index in log is %s", commitTo, this.getLastIndex());
        int oldCommit = this.getCommitIndex();
        if (commitTo > oldCommit) {
            this.commitIndex = commitTo;
            return this.getEntries(oldCommit + 1, commitTo + 1);
        }

        return Collections.emptyList();
    }

    public synchronized List<LogEntry> getEntriesNeedToApply() {
        int start = this.appliedIndex + 1;
        int end = this.getCommitIndex() + 1;

        assert start <= end : "start " + start + " end " + end;

        if (start == end) {
            return Collections.emptyList();
        } else {
            return this.getEntries(this.appliedIndex + 1, this.getCommitIndex() + 1);
        }
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
    public void shutdown() {
        pool.shutdown();
    }

    @Override
    public String toString() {
        return "{" +
                "commitIndex=" + commitIndex +
                ", appliedIndex=" + appliedIndex +
                '}';
    }
}
