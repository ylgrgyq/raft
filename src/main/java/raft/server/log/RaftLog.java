package raft.server.log;

import static com.google.common.base.Preconditions.*;
import raft.server.LogEntry;

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
    private int offset;
    private Storage storage;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private ArrayList<LogEntry> logs = new ArrayList<>();

    public RaftLog(Storage storage) {
        checkNotNull(storage);

//        int firstIndex = storage.getFirstIndex();
        int lastIndex = storage.getLastIndex();

        this.offset = lastIndex + 1;

        //TODO etcd set commitIndex and lastApplied to firstIndex
        this.commitIndex = this.offset;
        this.lastApplied = this.offset;

        this.storage = storage;

        this.logs.add(sentinel);
    }

    public synchronized void append(int term, LogEntry entry) {
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

    public int getTerm(int index) {

        return this.storage.getFirstIndex();
    }

    int getFirstIndex(){
//        this.
        return 0;
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

    public int lastIndex() {
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
        checkArgument(commitTo <= this.lastIndex(),
                "try commit to %s but last index in log is %s", commitTo, this.lastIndex());
        if (commitTo > this.getCommitIndex()) {
            this.commitIndex = commitTo;
            return true;
        }

        return false;
    }
}
