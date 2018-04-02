package raft.server.log;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;

import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class.getName());
    static final LogEntry sentinel = LogEntry.newBuilder().setTerm(0).setIndex(0).setData(ByteString.EMPTY).build();

    private int commitIndex = 0;
    private int appliedIndex = 0;
    private int offset;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private List<LogEntry> logs = new ArrayList<>();

    public RaftLog() {
        this.logs.add(sentinel);
        this.offset = this.getFirstIndex();
        this.commitIndex = this.offset;
        this.appliedIndex = this.offset;
    }

    public int getLastIndex() {
        return this.offset + this.logs.size() - 1;
    }

    public Optional<Integer> getTerm(int index) {
        if (index < this.offset || index > this.getLastIndex()) {
            return Optional.empty();
        }

        return Optional.of(this.logs.get(index - this.offset).getTerm());
    }

    private int getFirstIndex() {
        return this.logs.get(0).getIndex();
    }

    synchronized int truncate(int fromIndex) {
        checkArgument(fromIndex >= this.offset && fromIndex <= this.getCommitIndex(),
                "invalid truncate from: %s, current offset: %s, current commit index: %s",
                fromIndex, this.offset, this.getCommitIndex());

        logger.info("try truncating logs from {}, offset: {}, commitIndex: {}", fromIndex, this.offset, this.commitIndex);

        List<LogEntry> remainLogs = this.getEntries(fromIndex, this.getLastIndex() + 1);
        this.logs = new ArrayList<>();
        this.logs.addAll(remainLogs);
        assert !logs.isEmpty();
        this.offset = this.getFirstIndex();
        return this.getLastIndex();
    }

    public synchronized Optional<LogEntry> getEntry(int index) {
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public synchronized List<LogEntry> getEntries(int start, int end) {
        checkArgument(start >= this.offset && start < end, "invalid start and end: %s %s", start, end);

        start = start - this.offset;
        end = end - this.offset;
        return new ArrayList<>(this.logs.subList(start, Math.min(end, this.logs.size())));
    }

    public synchronized int directAppend(int term, List<byte[]> entries) {
        if (entries.size() == 0) {
            return this.getLastIndex();
        }

        int i = this.getLastIndex();
        for (byte[] data : entries) {
            ++i;
            LogEntry e = LogEntry.newBuilder()
                    .setData(ByteString.copyFrom(data))
                    .setIndex(i)
                    .setTerm(term)
                    .build();
            this.logs.add(e);
        }

        return this.getLastIndex();
    }

    public synchronized boolean tryAppendEntries(int prevIndex, int prevTerm, int leaderCommitIndex, List<LogEntry> entries) {
        checkArgument(!entries.isEmpty(),
                "try directAppend empty entries with prevIndex: %s, prevTerm: %s, leaderCommitIndex: %s",
                prevIndex, prevTerm, leaderCommitIndex);

        if (prevIndex < this.offset) {
            logger.warn("try directAppend entries with truncated prevIndex: {}. " +
                            "prevTerm: {}, leaderCommitIndex: {}, current offset: {}",
                    prevIndex, prevTerm, leaderCommitIndex, this.offset);
            return false;
        } else if (prevIndex > this.getLastIndex()) {
            logger.warn("try directAppend entries with out of range prevIndex: {}. " +
                            "prevTerm: {}, leaderCommitIndex: {}, current lastIndex: {}",
                    prevIndex, prevTerm, leaderCommitIndex, this.getLastIndex());
            return false;
        }

        if (this.match(prevTerm, prevIndex)) {
            int conflictIndex = this.searchConflict(entries);
            if (conflictIndex != 0) {
                if (conflictIndex <= this.commitIndex) {
                    logger.error("try directAppend entries conflict with committed entry on index: {}, " +
                                    "new entry: {}, committed entry: {}",
                            conflictIndex, entries.get(conflictIndex - prevIndex - 1), this.getEntry(conflictIndex));
                    throw new RuntimeException();
                }

                for (LogEntry e : entries.subList(conflictIndex - prevIndex - 1, entries.size())) {
                    int index = e.getIndex() - this.offset;
                    if (index >= this.logs.size()) {
                        this.logs.add(e);
                    } else {
                        this.logs.set(index, e);
                    }
                }
                int lastIndex = prevIndex + entries.size();
                if (this.tryCommitTo(Math.min(leaderCommitIndex, lastIndex)).isEmpty()) {
                    logger.warn("try commit to {} failed with current commitIndex: {} and lastIndex: {}",
                            Math.min(leaderCommitIndex, lastIndex), this.commitIndex, this.getLastIndex());
                }
            }
            return true;
        }

        return false;
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
        if (commitTo > this.getCommitIndex()) {
            this.commitIndex = commitTo;
            return this.getEntries(this.appliedIndex + 1, this.getCommitIndex() + 1);
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
    }
}
