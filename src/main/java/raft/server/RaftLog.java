package raft.server;

import static com.google.common.base.Preconditions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    private static final LogEntry sentinel = new LogEntry();
    private int commitIndex = 0;
    private int lastApplied = 0;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private ArrayList<LogEntry> logs = new ArrayList<>();

    RaftLog() {
        this.logs.add(sentinel);
    }

    void append(int term, LogEntry entry) {
        int lastIndex = this.lastIndex();
        entry.setIndex(lastIndex);
        entry.setTerm(term);
    }

    public void appendEntries(LogEntry entry) {

    }

    public Optional<LogEntry> getEntry(int index){
        checkArgument(index >= 0, "invalid index: %d", index);

        if (index > this.lastIndex()) {
            return Optional.empty();
        } else {
            return Optional.of(this.logs.get(index));
        }
    }

    public List<LogEntry> getEntries(int start, int end) {
        checkArgument(start >= 0 && end >= 1 && start < end, "invalid start and end: %d %d", start, end);

        return this.logs.subList(start, Math.min(end, this.logs.size()));
    }

    int lastIndex() {
        return this.logs.size() - 1;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public boolean tryCommitTo(int commitTo) {
        if (commitTo > getCommitIndex()) {
            this.commitIndex = Math.min(commitTo, lastIndex());
            return true;
        }

        return false;
    }
}
