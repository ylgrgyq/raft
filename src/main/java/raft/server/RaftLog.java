package raft.server;

import com.google.common.base.Preconditions;

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

    synchronized void append(int term, LogEntry entry) {
        int lastIndex = this.lastIndex();
        entry.setIndex(lastIndex);
        entry.setTerm(term);
        this.logs.add(entry);
    }

    public synchronized void appendEntries(List<LogEntry> entries) {
//        If an existing entry conflicts with a new one (same index
//        but different terms), delete the existing entry and all that
//        follow it (§5.3)

//        Append any new entries not already in the log
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

    int lastIndex() {
        return this.logs.size() - 1;
    }

    public synchronized boolean isUpToDate(int term, int index) {
        LogEntry lastEntryOnServer = this.getEntry(this.lastIndex()).orElse(null);
        assert lastEntryOnServer != null;

        return term > lastEntryOnServer.getTerm() ||
                (term == lastEntryOnServer.getTerm() &&
                        index >= lastEntryOnServer.getIndex());
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public synchronized boolean tryCommitTo(int commitTo) {
        Preconditions.checkArgument(commitTo <= this.lastIndex(),
                "try commit to %s but last index in log is %s", commitTo, this.lastIndex());
        if (commitTo > this.getCommitIndex()) {
            this.commitIndex = commitTo;
            return true;
        }

        return false;
    }
}
