package raft.server.log;

import com.google.protobuf.ByteString;
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

    static final LogEntry sentinel = LogEntry.newBuilder().setTerm(0).setIndex(0).setData(ByteString.EMPTY).build();

    private final ExecutorService pool;
    private final PersistentStorage storage;

    private int commitIndex;
    private int appliedIndex;

    public RaftLogImpl(PersistentStorage storage) {
        this.storage = storage;
        this.pool = Executors.newSingleThreadExecutor(defaultThreadFactory);
    }

    @Override
    public void init() {
        storage.init();

        int firstIndex = storage.getFirstIndex();
        this.commitIndex = firstIndex;
        this.appliedIndex = firstIndex;
    }

    public int getLastIndex() {
        return storage.getLastIndex();
    }

    public Optional<Integer> getTerm(int index) {
        return storage.getTerm(index);
    }

    public synchronized Optional<LogEntry> getEntry(int index) {
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public List<LogEntry> getEntries(int start, int end) {
        return storage.getEntries(start, end);
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

                storage.append(entries.subList(conflictIndex - prevIndex - 1, entries.size()));
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

    public int getCommitIndex() {
        return this.commitIndex;
    }

    public int getAppliedIndex() {
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
    }

    @Override
    public void shutdown() {
        storage.shutdown();
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
