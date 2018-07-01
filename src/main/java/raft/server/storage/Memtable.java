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
    private ConcurrentSkipListMap<Integer, LogEntry> table;
    private int memSize;

    Memtable() {
        table = new ConcurrentSkipListMap<>();
    }

    void add(int k, LogEntry v) {
        table.put(k, v);
        memSize += Integer.BYTES + v.getSerializedSize();
    }

    Integer firstKey() {
        return table.firstKey();
    }

    Integer lastKey() {
        return table.lastKey();
    }

    boolean isEmpty() {
        return table.isEmpty();
    }

    LogEntry get(int k) {
        return table.get(k);
    }

    Set<Map.Entry<Integer, LogEntry>> entrySet(){
        return table.entrySet();
    }

    List<LogEntry> getEntries(int start, int end) {
        if (end < firstKey() || start > lastKey()) {
            return Collections.emptyList();
        }

        SeekableIterator<LogEntry> iter = iterator();
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
    public SeekableIterator<LogEntry> iterator() {
        return new Itr(table.clone());
    }

    private class Itr implements SeekableIterator<LogEntry> {
        private ConcurrentNavigableMap<Integer, LogEntry> innerMap;
        private Map.Entry<Integer, LogEntry> offset;

        Itr(ConcurrentNavigableMap<Integer, LogEntry> innerMap) {
            this.innerMap = innerMap;
            this.offset = innerMap.firstEntry();
        }

        @Override
        public void seek(int key) {
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
