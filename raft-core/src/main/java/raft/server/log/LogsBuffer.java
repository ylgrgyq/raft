package raft.server.log;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/5/27
 */
class LogsBuffer {
    private static final Logger logger = LoggerFactory.getLogger(LogsBuffer.class.getName());

    private List<LogEntry> logsBuffer;
    private long offsetIndex;

    LogsBuffer(LogEntry offsetEntry) {
        logsBuffer = new ArrayList<>();
        logsBuffer.add(offsetEntry);
        this.offsetIndex = offsetEntry.getIndex();
    }

    synchronized long getLastIndex() {
        return offsetIndex + logsBuffer.size() - 1;
    }

    synchronized long getTerm(long index) {
        assert index >= offsetIndex && index < offsetIndex + logsBuffer.size() :
                String.format("index:%s offsetIndex:%s logsBufferSize:%s", index, offsetIndex, logsBuffer.size());

        return logsBuffer.get((int)(index - offsetIndex)).getTerm();
    }

    synchronized long getOffsetIndex() {
        return offsetIndex;
    }

    synchronized List<LogEntry> getEntries(long start, long end) {
        assert start >= offsetIndex && end > start;
        assert start < offsetIndex + logsBuffer.size();
        assert end <= offsetIndex + logsBuffer.size();

        return logsBuffer.subList(Math.max(0, (int)(start - offsetIndex)), (int)(end - offsetIndex));
    }

    synchronized void append(List<LogEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        long firstIndex = entries.get(0).getIndex();
        if (firstIndex == offsetIndex + logsBuffer.size()) {
            // normal append
            logsBuffer.addAll(entries);
        } else if (firstIndex <= offsetIndex) {
            logger.warn("replace entire buffer logs from index: {}", firstIndex);
            offsetIndex = firstIndex;
            logsBuffer = new ArrayList<>(entries);
        } else {
            logger.warn("replace buffer logs from index: {}", firstIndex);
            logsBuffer = new ArrayList<>();
            logsBuffer.addAll(getEntries(offsetIndex, firstIndex));
            logsBuffer.addAll(entries);
        }
    }

    synchronized void truncateBuffer(long index) {
        logger.debug("try truncating logs from {}, current offset:{}", index, offsetIndex);

        assert index <= getLastIndex();
        assert index < offsetIndex + logsBuffer.size();

        if (index > offsetIndex) {
            // always at least keep last log entry in buffer
            List<LogEntry> remainLogs = logsBuffer.subList((int)(index - offsetIndex), logsBuffer.size());
            logsBuffer = new ArrayList<>();
            logsBuffer.addAll(remainLogs);


            offsetIndex = index;
        }
    }

    synchronized void installSnapshot(LogSnapshot snapshot) {
        assert snapshot != null;

        long index = snapshot.getIndex();
        if (index > getLastIndex()) {
            logsBuffer = new ArrayList<>();
            offsetIndex = snapshot.getIndex();
            LogEntry entry = LogEntry.newBuilder()
                    .setIndex(snapshot.getIndex())
                    .setTerm(snapshot.getTerm())
                    .build();
            logsBuffer.add(entry);
        } else if (index > offsetIndex) {
            truncateBuffer(index);
        }
    }
}
