package raft.server.log;

import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
public class MemoryFakePersistentStorage implements PersistentStorage{
    private static final Logger logger = LoggerFactory.getLogger(PersistentStorage.class.getName());
    static final LogEntry sentinel = LogEntry.newBuilder().setTerm(0).setIndex(0).setData(ByteString.EMPTY).build();

    // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
    private List<LogEntry> logs = new ArrayList<>();
    private int offset;

    public MemoryFakePersistentStorage(){
        this.logs.add(sentinel);
        this.offset = this.getFirstIndex();
    }

    @Override
    public void init() {
        logger.warn("Using memory based storage, your data could be lost. Please use it only in testing environment");
    }

    @Override
    public int getLastIndex() {
        return this.offset + this.logs.size() - 1;
    }

    @Override
    public Optional<Integer> getTerm(int index) {
        if (index < this.offset || index > this.getLastIndex()) {
            return Optional.empty();
        }

        return Optional.of(this.logs.get(index - this.offset).getTerm());
    }

    @Override
    public int getFirstIndex() {
        return this.logs.get(0).getIndex();
    }

    @Override
    public List<LogEntry> getEntries(int start, int end) {
        checkArgument(start >= this.offset && start < end, "invalid start and end: %s %s", start, end);

        start = start - this.offset;
        end = end - this.offset;
        return new ArrayList<>(this.logs.subList(start, Math.min(end, this.logs.size())));
    }

    @Override
    public void append(List<LogEntry> entries) {
        for (LogEntry e : entries) {
            int index = e.getIndex() - this.offset;
            if (index >= this.logs.size()) {
                this.logs.add(e);
            } else {
                this.logs.set(index, e);
            }
        }
    }

    synchronized int truncate(int fromIndex) {
//        checkArgument(fromIndex >= this.offset && fromIndex <= this.getCommitIndex(),
//                "invalid truncateBuffer from: %s, current offset: %s, current commit index: %s",
//                fromIndex, this.offset, this.getCommitIndex());
//
//        logger.info("try truncating logs from {}, offset: {}, commitIndex: {}", fromIndex, this.offset, this.commitIndex);
//
//        List<LogEntry> remainLogs = this.getEntries(fromIndex, this.getLastIndex() + 1);
//        this.logs = new ArrayList<>();
//        this.logs.addAll(remainLogs);
//        assert !logs.isEmpty();
//        this.offset = this.getFirstIndex();
//        return this.getLastIndex();
        return 0;
    }
}
