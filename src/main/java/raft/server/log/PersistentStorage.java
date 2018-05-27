package raft.server.log;

import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public interface PersistentStorage {
    void init();

    int getLastIndex();

    Optional<Integer> getTerm(int index);

    int getFirstIndex();

    List<LogEntry> getEntries(int start, int end);

    void append(List<LogEntry> entries);

    void shutdown();
}
