package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public class MemoryBasedTestingStorage implements PersistentStorage {
    private static final Logger logger = LoggerFactory.getLogger(PersistentStorage.class.getName());

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private List<LogEntry> logs = new ArrayList<>();
    private long offset;

    public MemoryBasedTestingStorage(){
        this.logs.add(PersistentStorage.sentinel);
        this.offset = getFirstIndex();
    }

    @Override
    public void init() {
        logger.warn("Using memory based storage, your data could be lost. Please use it only in testing environment");
    }

    @Override
    public synchronized long getLastIndex() {
        return offset + logs.size() - 1;
    }

    @Override
    public synchronized long getTerm(long index){
        if (index < offset) {
            throw new LogsCompactedException(index);
        }

        long lastIndex = getLastIndex();
        checkArgument(index <= lastIndex,
                "index: %s out of bound, lastIndex: %s",
                index, lastIndex);

        return logs.get((int)(index - offset)).getTerm();
    }

    @Override
    public synchronized long getFirstIndex() {

        return logs.get(0).getIndex();
    }

    @Override
    public synchronized List<LogEntry> getEntries(long start, long end){
        checkArgument(start < end, "invalid start and end: %s %s", start, end);

        if (start < offset) {
            throw new LogsCompactedException(start);
        }

        start = start - this.offset;
        end = end - this.offset;
        return new ArrayList<>(this.logs.subList((int)start, Math.min((int)end, this.logs.size())));
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        long firstIndex = entries.get(0).getIndex();
        if (firstIndex == offset + logs.size()) {
            // normal append
            logs.addAll(entries);
        } else if (firstIndex <= offset) {
            logger.warn("replace entire buffer logs from index: {}", firstIndex);
            offset = firstIndex;
            logs = new ArrayList<>(entries);
        } else {
            logger.warn("replace buffer logs from index: {}", firstIndex);
            logs = new ArrayList<>();
            logs.addAll(getEntries(offset, firstIndex));
            logs.addAll(entries);
        }
    }

    public synchronized Future<Long> compact(long toIndex) {
        checkArgument(toIndex <= getLastIndex(),
                "compactIndex: %s should lower than last index: %s",
                toIndex, getLastIndex());

        if (toIndex < getFirstIndex()) {
            throw new LogsCompactedException(toIndex);
        }

        if (toIndex > offset) {
            // always at least keep last log entry in buffer
            List<LogEntry> remainLogs = logs.subList((int)(toIndex - offset), logs.size());
            logs = new ArrayList<>();
            logs.addAll(remainLogs);


            offset = toIndex;
        }
        return CompletableFuture.completedFuture(offset);
    }

    @Override
    public void shutdown() {

    }
}
