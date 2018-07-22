package raft.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class TableCache {
    private String baseDir;
    private String storageName;
    private Cache<Integer, Table> cache;

    TableCache(String baseDir, String storageName) {
        this.baseDir = baseDir;
        this.storageName = storageName;
        cache = CacheBuilder.newBuilder()
                .maximumSize(1024)
                .build();
    }

    List<LogEntry> getEntries(int fileNumber, long fileSize, int startKey, int endKey) throws IOException {
        Table t = findTable(fileNumber, fileSize);
        return t.getEntries(startKey, endKey);
    }

    private Table findTable(int fileNumber, long fileSize) throws IOException {
        Table t = cache.getIfPresent(fileNumber);
        if (t == null) {
            t = loadTable(fileNumber, fileSize);
        }
        return t;
    }

    Table loadTable(int fileNumber, long fileSize) throws IOException {
        String tableFileName = FileName.getSSTableName(storageName, fileNumber);
        FileChannel ch = FileChannel.open(Paths.get(baseDir, tableFileName), StandardOpenOption.READ);
        Table t = Table.open(ch, fileSize);
        cache.put(fileNumber, t);
        return t;
    }

    SeekableIterator<LogEntry> iterator(int fileNumber, long fileSize) throws IOException {
        Table t = findTable(fileNumber, fileSize);
        return t.iterator();
    }

    void evict(int fileNumber) throws IOException {
        Table t = cache.getIfPresent(fileNumber);
        if (t != null) {
            t.close();
        }
        cache.invalidate(fileNumber);
    }

    void evictAll() throws IOException {
        for(Map.Entry<Integer, Table> e : cache.asMap().entrySet()) {
            e.getValue().close();
        }
        cache.invalidateAll();
    }
}
