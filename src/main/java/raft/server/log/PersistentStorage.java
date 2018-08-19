package raft.server.log;

import com.google.protobuf.ByteString;
import raft.server.proto.LogEntry;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public interface PersistentStorage {
    LogEntry sentinel = LogEntry.newBuilder().setTerm(0).setIndex(0).setData(ByteString.copyFrom(new byte[1])).build();

    void init();

    /**
     * Get the index of the last entry in the storage. The default value is -1 when this storage is empty.
     *
     * @return the index of the last entry in storage or -1 when storage is empty
     */
    int getLastIndex();

    /**
     * Search the LogEntry in this storage by the index then return the term of this retrieved LogEntry
     *
     * @param index index of the searched LogEntry
     * @return the term of the searched LogEntry or -1 when target LogEntry is not found
     */
    int getTerm(int index);

    /**
     * Get the index of the first entry in the storage. The default value is -1 when this storage is empty.
     *
     * @return the index of the first entry in storage or -1 when storage is empty
     */
    int getFirstIndex();

    List<LogEntry> getEntries(int start, int end);

    void append(List<LogEntry> entries);


    CompletableFuture<Void> compact(int toIndex);

    void shutdown();
}
