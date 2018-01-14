package raft.server;

import java.util.ArrayList;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    int commitIndex;
    int appliedIndex;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private ArrayList<LogEntry> logs = new ArrayList<>();

    void append(int term, LogEntry entry) {
        int lastIndex = lastIndex();
        entry.setIndex(lastIndex);
        entry.setTerm(term);
    }

    LogEntry getEntry(int index){
        return logs.get(index);
    }

    int lastIndex(){
        return 1;
    }
}
