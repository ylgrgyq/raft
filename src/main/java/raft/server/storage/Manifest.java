package raft.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Manifest {
    private static final Logger logger = LoggerFactory.getLogger(Manifest.class.getName());

    private final BlockingQueue<CompactTask<Void>> compactTaskQueue;
    private final String baseDir;
    private final String storageName;
    private final List<SSTableFileMetaInfo> metas;
    private final ReentrantLock metasLock;

    private int nextFileNumber = 1;
    private int logNumber;
    private LogWriter manifestRecordWriter;
    private int manifestFileNumber;

    Manifest(String baseDir, String storageName) {
        this.baseDir = baseDir;
        this.storageName = storageName;
        this.metas = new CopyOnWriteArrayList<>();
        this.compactTaskQueue = new LinkedBlockingQueue<>();
        this.metasLock = new ReentrantLock();
    }

    private void registerMetas(List<SSTableFileMetaInfo> metas) {
        metasLock.lock();
        try {
            this.metas.addAll(metas);
        }finally {
            metasLock.unlock();
        }
    }

    synchronized void logRecord(ManifestRecord record) throws IOException {
        assert record.getType() != ManifestRecord.Type.PLAIN || record.getLogNumber() >= logNumber;

        registerMetas(record.getMetas());

        String manifestFileName = null;
        if (manifestRecordWriter == null) {
            manifestFileNumber = getNextFileNumber();
            manifestFileName = FileName.getManifestFileName(storageName, manifestFileNumber);
            FileChannel manifestFile = FileChannel.open(Paths.get(baseDir, manifestFileName),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            manifestRecordWriter = new LogWriter(manifestFile);
        }

        if (record.getType() == ManifestRecord.Type.PLAIN) {
            record.setNextFileNumber(nextFileNumber);
        }
        manifestRecordWriter.append(record.encode());
        manifestRecordWriter.flush();

        logger.debug("written manifest record {} to manifest file number {}", record, manifestFileNumber);

        if (manifestFileName != null) {
            FileName.setCurrentFile(baseDir, storageName, manifestFileNumber);
        }
    }

    void processCompactTask() throws IOException {
        int greatestToKey = -1;
        List<CompactTask<Void>> tasks = new ArrayList<>();
        CompactTask<Void> task;
        while ((task = compactTaskQueue.poll()) != null){
            tasks.add(task);
            if (task.getToKey() > greatestToKey) {
                greatestToKey = task.getToKey();
            }
        }

        if (greatestToKey > 0) {
            List<SSTableFileMetaInfo> remainMetas = searchMetas(greatestToKey + 1, Integer.MAX_VALUE);
            if (remainMetas.size() < metas.size()) {
                ManifestRecord record = ManifestRecord.newReplaceAllExistedMetasRecord();
                record.addMetas(remainMetas);
                logRecord(record);

                metasLock.lock();
                try {
                    metas.clear();
                    metas.addAll(remainMetas);
                } finally {
                    metasLock.unlock();
                }
            } else {
                assert remainMetas.size() == metas.size();
            }
        }

        tasks.forEach(t -> t.getFuture().complete(null));
    }

    CompletableFuture<Void> compact(int toKey) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        CompactTask<Void> task = new CompactTask<>(future, toKey);
        compactTaskQueue.add(task);
        return future;
    }

    synchronized void recover(String manifestFileName) throws IOException {
        try (FileChannel manifestFile = FileChannel.open(Paths.get(baseDir, manifestFileName),
                StandardOpenOption.READ)) {
            LogReader reader = new LogReader(manifestFile);
            List<SSTableFileMetaInfo> ms = new ArrayList<>();
            while (true) {
                Optional<byte[]> logOpt = reader.readLog();
                if (logOpt.isPresent()) {
                    ManifestRecord record = ManifestRecord.decode(logOpt.get());
                    if (record.getType() == ManifestRecord.Type.PLAIN) {
                        nextFileNumber = record.getNextFileNumber();
                        logNumber = record.getLogNumber();
                        ms.addAll(record.getMetas());
                    } else {
                        ms = new ArrayList<>(record.getMetas());
                    }
                } else {
                    break;
                }
            }

            metasLock.lock();
            try {
                // we must make sure that searchMetas will only be called after recovery
                assert metas.isEmpty();
                metas.addAll(ms);
            } finally {
                metasLock.unlock();
            }
        }
    }

    int getFirstIndex() {
        metasLock.lock();
        try {
            if (!metas.isEmpty()) {
                return metas.get(0).getFirstKey();
            } else {
                return -1;
            }
        } finally {
            metasLock.unlock();
        }
    }

    int getLastIndex() {
        metasLock.lock();
        try {
            if (!metas.isEmpty()) {
                return metas.get(metas.size() - 1).getLastKey();
            } else {
                return -1;
            }
        } finally {
            metasLock.unlock();
        }
    }

    int getLowestSSTableFileNumber() {
        metasLock.lock();
        try {
            return metas.stream()
                    .mapToInt(SSTableFileMetaInfo::getFileNumber)
                    .min()
                    .orElse(-1);
        } finally {
            metasLock.unlock();
        }
    }

    synchronized void close() throws IOException {
        if (manifestRecordWriter != null) {
            manifestRecordWriter.close();
        }
    }

    synchronized int getNextFileNumber() {
        return nextFileNumber++;
    }

    synchronized int getLogFileNumber() {
        return logNumber;
    }

    /**
     * find all the SSTableFileMetaInfo who's index range intersect with startIndex and endIndex
     *
     * @param startKey target start key (inclusive)
     * @param endKey target end key (exclusive)
     *
     * @return iterator for found SSTableFileMetaInfo
     */
    List<SSTableFileMetaInfo> searchMetas(int startKey, int endKey) {
        metasLock.lock();
        try {
            int startMetaIndex;
            if (metas.size() > 32) {
                startMetaIndex = binarySearchStartMeta(startKey);
            } else {
                startMetaIndex = traverseSearchStartMeta(startKey);
            }

            return metas.subList(startMetaIndex, metas.size())
                    .stream()
                    .filter(meta -> meta.getFirstKey() < endKey)
                    .collect(Collectors.toList());
        } finally {
            metasLock.unlock();
        }
    }

    private int traverseSearchStartMeta(int index) {
        int i = 0;
        while (i < metas.size()) {
            SSTableFileMetaInfo meta = metas.get(i);
            if (index <= meta.getFirstKey()) {
                break;
            } else if (index <= meta.getLastKey()) {
                break;
            }
            ++i;
        }

        return i;
    }

    private int binarySearchStartMeta(int index) {
        int start = 0;
        int end = metas.size();

        while (start < end) {
            int mid = (start + end) / 2;
            SSTableFileMetaInfo meta = metas.get(mid);
            if (index >= meta.getFirstKey() && index <= meta.getLastKey()) {
                return mid;
            } else if (index < meta.getFirstKey()) {
                end = mid;
            } else {
                start = mid + 1;
            }
        }

        return start;
    }

    private static class CompactTask <T> {
        private final CompletableFuture<T> future;
        private final int toKey;

        CompactTask(CompletableFuture<T> future, int toKey) {
            this.future = future;
            this.toKey = toKey;
        }

        CompletableFuture<T> getFuture() {
            return future;
        }

        int getToKey() {
            return toKey;
        }
    }
}
