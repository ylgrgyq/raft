package raft.server.storage;

import raft.server.proto.LogEntry;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Author: ylgrgyq
 * Date: 18/6/11
 */
class Memtable implements Iterable<LogEntry> {
    private ConcurrentSkipListMap<Long, LogEntry> table;
    private int memSize;

    Memtable() {
        table = new ConcurrentSkipListMap<>();
    }

    void add(long k, LogEntry v) {
//        assert k > 0;

        if (! table.isEmpty() && k <= table.lastKey()){
            long removeStartKeyNotInclusive = table.ceilingKey(k);
            table = new ConcurrentSkipListMap<>(table.subMap(-1L, removeStartKeyNotInclusive));

            memSize = recalculateCurrentMemoryUsedInBytes();
        }

        table.put(k, v);
        memSize += Long.BYTES + v.getSerializedSize();
    }

    private int recalculateCurrentMemoryUsedInBytes() {
        int entrySize = table.values().stream().mapToInt(LogEntry::getSerializedSize).sum();
        return table.size() * Long.BYTES + entrySize;
    }

    Long firstKey() {
        if (table.isEmpty()) {
            return -1L;
        } else {
            return table.firstKey();
        }
    }

    Long lastKey() {
        if (table.isEmpty()) {
            return -1L;
        } else {
            return table.lastKey();
        }
    }

    boolean isEmpty() {
        return table.isEmpty();
    }

    LogEntry get(long k) {
        return table.get(k);
    }

    List<LogEntry> getEntries(long start, long end) {
        if (end < firstKey() || start > lastKey()) {
            return Collections.emptyList();
        }

        SeekableIterator<Long, LogEntry> iter = iterator();
        iter.seek(start);

        List<LogEntry> ret = new ArrayList<>();
        while (iter.hasNext()) {
            LogEntry v = iter.next();
            if (v.getIndex() >= start && v.getIndex() < end) {
                ret.add(v);
            } else {
                break;
            }
        }

        return ret;
    }

    int getMemoryUsedInBytes(){
        return memSize;
    }

    @Override
    public SeekableIterator<Long, LogEntry> iterator() {
        return new Itr(table.clone());
    }

    private static class Itr implements SeekableIterator<Long, LogEntry> {
        private ConcurrentNavigableMap<Long, LogEntry> innerMap;
        private Map.Entry<Long, LogEntry> offset;

        Itr(ConcurrentNavigableMap<Long, LogEntry> innerMap) {
            this.innerMap = innerMap;
            this.offset = innerMap.firstEntry();
        }

        @Override
        public void seek(Long key) {
            offset = innerMap.ceilingEntry(key);
        }

        @Override
        public boolean hasNext() {
            return offset != null;
        }

        @Override
        public LogEntry next() {
            LogEntry v = offset.getValue();
            offset = innerMap.higherEntry(v.getIndex());
            return v;
        }
    }
}
