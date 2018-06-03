package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public class MemoryFakePersistentStorage implements PersistentStorage {
    private static final Logger logger = LoggerFactory.getLogger(PersistentStorage.class.getName());

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private List<LogEntry> logs = new ArrayList<>();
    private int offset;

    public MemoryFakePersistentStorage(){
        this.logs.add(PersistentStorage.sentinel);
        this.offset = getFirstIndex();
    }

    @Override
    public void init() {
        logger.warn("Using memory based storage, your data could be lost. Please use it only in testing environment");
    }

    @Override
    public synchronized int getLastIndex() {
        return offset + logs.size() - 1;
    }

    @Override
    public synchronized int getTerm(int index){
        if (index < offset) {
            throw new LogsCompactedException(index);
        }

        int lastIndex = getLastIndex();
        checkArgument(index <= lastIndex,
                "index: %s out of bound, lastIndex: %s",
                index, lastIndex);

        return logs.get(index - offset).getTerm();
    }

    @Override
    public synchronized int getFirstIndex() {
        return logs.get(0).getIndex();
    }

    @Override
    public synchronized List<LogEntry> getEntries(int start, int end){
        checkArgument(start < end, "invalid start and end: %s %s", start, end);

        if (start < offset) {
            throw new LogsCompactedException(start);
        }

        start = start - this.offset;
        end = end - this.offset;
        return new ArrayList<>(this.logs.subList(start, Math.min(end, this.logs.size())));
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        int firstIndex = entries.get(0).getIndex();
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

    public synchronized void compact(int toIndex) {
        checkArgument(toIndex <= getLastIndex(),
                "compactIndex: %s should lower than last index: %s",
                toIndex, getLastIndex());

        if (toIndex < getFirstIndex()) {
            throw new LogsCompactedException(toIndex);
        }

        if (toIndex > offset) {
            // always at least keep last log entry in buffer
            List<LogEntry> remainLogs = logs.subList(toIndex - offset, logs.size());
            logs = new ArrayList<>();
            logs.addAll(remainLogs);


            offset = toIndex;
        }
    }
}
