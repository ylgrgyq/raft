package raft.server;

import java.util.ArrayList;

/**
 * Author: ylgrgyq
 * Date: 18/1/8
 */
public class RaftLog {
    long commitIndex;
    long appliedIndex;

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private ArrayList<LogEntry> logs = new ArrayList<>();

    void append(int term, LogEntry entry) {

    }

    int lastIndex(){
        return 1;
    }
}
