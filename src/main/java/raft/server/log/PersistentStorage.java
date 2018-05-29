package raft.server.log;

import com.google.protobuf.ByteString;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Optional;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public interface PersistentStorage {
    LogEntry sentinel = LogEntry.newBuilder().setTerm(0).setIndex(0).setData(ByteString.EMPTY).build();

    void init();

    /**
     * Get the index of the last entry in the storage. Please note that the last entry should be retrievable
     * by getEntries even on the first run of the storage service or after a compaction.
     *
     * @return the index of the last entry in storage
     */
    int getLastIndex();

    int getTerm(int index);

    /**
     * Get the index of the first entry in the storage. Please note that the first entry should be retrievable
     * by getEntries even on the first run of the storage service or after a compaction.
     *
     * @return the index of the first entry in storage
     */
    int getFirstIndex();

    List<LogEntry> getEntries(int start, int end);

    void append(List<LogEntry> entries);

    void compact(int toIndex);
}
