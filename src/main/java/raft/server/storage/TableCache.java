package raft.server.storage;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class TableCache<V> {
    private String baseDir;
    private String storageName;
    private Cache<Integer, Table> cache;

    TableCache(String baseDir, String storageName) {
        this.storageName = storageName;
        cache = CacheBuilder.newBuilder()
                .maximumSize(1024)
                .build();
    }

    V get(int fileNumber, long fileSize, int key) {
        Table t = cache.getIfPresent(fileNumber);
        return null;
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

    public void evict(int fileNumber){
        cache.invalidate(fileNumber);
    }
}
