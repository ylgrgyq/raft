package raft.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    private static final Logger logger = LoggerFactory.getLogger(RaftLog.class.getName());

    private static final LogEntry sentinel = new LogEntry();
    private int commitIndex = 0;
    private int offset;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private List<LogEntry> logs = new ArrayList<>();

    public RaftLog() {
        this.offset = this.getFirstIndex();

        this.commitIndex = this.offset - 1;

        this.logs.add(sentinel);
    }

    public synchronized int append(List<LogEntry> entries) {
        if (entries.size() == 0) {
            return this.getLastIndex();
        }

        int i = this.getLastIndex();
        for (LogEntry e : entries) {
            ++i;
            e.setIndex(i);
            this.logs.add(e);
        }

        return this.getLastIndex();
    }

    public synchronized Optional<Integer> tryAppendEntries(int prevIndex, int prevTerm, int leaderCommitIndex, List<LogEntry> entries) {
        if (this.commitIndex > prevIndex) {
            if (this.commitIndex >= prevIndex + entries.size()){
                return Optional.of(this.commitIndex);
            } else {
                int from = commitIndex - prevIndex;
                entries = entries.subList(from, entries.size());
                prevIndex = commitIndex;
                prevTerm = entries.get(from - 1).getTerm();
            }
        }

        if (this.match(prevTerm, prevIndex)) {
            int conflictIndex = this.searchConflict(entries);
            if (conflictIndex != 0) {
                assert conflictIndex > this.commitIndex;

                for (LogEntry e : entries.subList(conflictIndex - prevIndex - 1, entries.size())) {
                    int index = e.getIndex() - this.offset;
                    this.logs.set(index, e);
                }
                int lastIndex = prevIndex + entries.size();
                this.tryCommitTo(Math.min(leaderCommitIndex, lastIndex));
                return Optional.of(lastIndex);
            }
        }

        return Optional.empty();
    }

    private int searchConflict(List<LogEntry> entries) {
        for (LogEntry entry : entries) {
            if (! this.match(entry.getTerm(), entry.getIndex())) {
                if (entry.getIndex() <= this.getLastIndex()) {
                    logger.warn("found conflict entry at index %s, existing term: %s, conflicting term: %s",
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

    public Optional<Integer> getTerm(int index) {
        if (index < this.getFirstIndex() - 1 || index > this.getLastIndex()) {
            return Optional.empty();
        }

        if (index > this.offset) {
            return Optional.of(this.logs.get(index - this.offset).getTerm());
        }

        return Optional.empty();
    }

    private int getFirstIndex(){
        return this.logs.get(0).getIndex();
    }

    public synchronized Optional<LogEntry> getEntry(int index){
        List<LogEntry> entries = getEntries(index, index + 1);

        if (entries.isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(entries.get(0));
        }
    }

    public synchronized List<LogEntry> getEntries(int start, int end) {
        checkArgument(start >= 0 && end >= 1 && start < end, "invalid start and end: %d %d", start, end);

        return this.logs.subList(start, Math.min(end, this.logs.size()));
    }

    public int getLastIndex() {
        return this.logs.size() - 1;
    }

    public synchronized boolean isUpToDate(int term, int index) {
        LogEntry lastEntryOnServer = this.getEntry(this.getLastIndex()).orElse(null);
        assert lastEntryOnServer != null;

        return term > lastEntryOnServer.getTerm() ||
                (term == lastEntryOnServer.getTerm() &&
                        index >= lastEntryOnServer.getIndex());
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public synchronized boolean tryCommitTo(int commitTo) {
        checkArgument(commitTo <= this.getLastIndex(),
                "try commit to %s but last index in log is %s", commitTo, this.getLastIndex());
        if (commitTo > this.getCommitIndex()) {
            this.commitIndex = commitTo;
            return true;
        }

        return false;
    }
}
