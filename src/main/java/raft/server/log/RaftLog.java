package raft.server.log;

import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/5/16
 */
public interface RaftLog {
    int getLastIndex();

    Optional<Integer> getTerm(int index);

    Optional<LogEntry> getEntry(int index);

    List<LogEntry> getEntries(int start, int end);

    int directAppend(int term, List<LogEntry> entries);

    int tryAppendEntries(int prevIndex, int prevTerm, List<LogEntry> entries);

    boolean isUpToDate(int term, int index);

    int getCommitIndex();

    int getAppliedIndex();

    List<LogEntry> tryCommitTo(int commitTo);

    List<LogEntry> getEntriesNeedToApply();

    void appliedTo(int appliedTo);
}
