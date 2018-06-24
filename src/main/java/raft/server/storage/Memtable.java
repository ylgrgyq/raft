package raft.server.storage;

import raft.server.proto.LogEntry;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * Author: ylgrgyq
 * Date: 18/6/11
 */
class Memtable {
    private ConcurrentSkipListMap<Integer, LogEntry> table;
    private int memSize;

    Memtable() {
        table = new ConcurrentSkipListMap<>();
    }

    void add(int k, LogEntry v) {
        table.put(k, v);
        memSize += Integer.BYTES + v.getSerializedSize();
    }

    int firstKey() {
        return table.firstKey();
    }

    int lastKey() {
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

    ConcurrentNavigableMap<Integer, LogEntry> subMap(int start, int end) {
        return table.subMap(start, end);
    }

    int getMemoryUsedInBytes(){
        return memSize;
    }
}
