package raft.server.storage;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.log.StorageInternalError;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.*;

/**
 * Author: ylgrgyq
 * Date: 18/6/8
 */
public class FileBasedStorage implements PersistentStorage {
    private static final Logger logger = LoggerFactory.getLogger(FileBasedStorage.class.getName());

    private final ExecutorService sstableWriterPool = Executors.newSingleThreadScheduledExecutor(
            new ThreadFactoryImpl("SSTable-Writer-"));
    private final String baseDir;
    private final String storageName;
    private final TableCache tableCache;
    private final Manifest manifest;

    private FileChannel storageLockChannel;
    private FileLock storageLock;
    private LogWriter logWriter;
    private int logFileNumber;
    private int firstIndexInStorage;
    private int lastIndexInStorage;
    private Memtable mm;
    private Memtable imm;
    private volatile StorageStatus status;
    private boolean backgroundWriteSstableRunning;

    public FileBasedStorage(String storageBaseDir, String storageName) {
        checkNotNull(storageBaseDir);
        Path baseDirPath = Paths.get(storageBaseDir);

        checkArgument(Files.notExists(baseDirPath) || Files.isDirectory(baseDirPath),
                "\"%s\" must be a directory to hold raft persistent logs", storageBaseDir);
        checkArgument(storageName.matches("[A-Za-z0-9]+"),
                "storage name must not empty and can only contains english letters and numbers, actual:\"%s\"",
                storageName);

        this.mm = new Memtable();
        this.storageName = storageName;
        this.baseDir = storageBaseDir + "/" + storageName;
        this.firstIndexInStorage = -1;
        this.lastIndexInStorage = -1;
        this.status = StorageStatus.NEED_INIT;
        this.tableCache = new TableCache(baseDir, storageName);
        this.manifest = new Manifest(baseDir, storageName);
        this.backgroundWriteSstableRunning = false;
    }

    @Override
    public synchronized void init() {
        checkState(status == StorageStatus.NEED_INIT,
                "storage don't need initialization. current status: %s", status);
        try {
            createStorageDir();

            Path lockFilePath = Paths.get(baseDir, FileName.getLockFileName(storageName));
            storageLockChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            storageLock = storageLockChannel.tryLock();
            checkState(storageLock != null,
                    "file storage: \"%s\" is occupied by other process", baseDir);

            logger.debug("start init storage {} under {}", storageName, baseDir);

            ManifestRecord record = ManifestRecord.newPlainRecord();
            if (Files.exists(Paths.get(baseDir, FileName.getCurrentManifestFileName(storageName)))) {
                recoverStorage(record);
            }

            if (logWriter == null) {
                int nextLogFileNumber = manifest.getNextFileNumber();
                String nextLogFile = FileName.getLogFileName(storageName, nextLogFileNumber);
                FileChannel logFile = FileChannel.open(Paths.get(baseDir, nextLogFile),
                        StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                logWriter = new LogWriter(logFile);
                logFileNumber = nextLogFileNumber;
            }

            record.setLogNumber(logFileNumber);
            manifest.logRecord(record);

            firstIndexInStorage = manifest.getFirstIndex();
            if (firstIndexInStorage < 0) {
                firstIndexInStorage = mm.firstKey();
            }

            lastIndexInStorage = mm.lastKey();
            if (lastIndexInStorage < 0) {
                lastIndexInStorage = manifest.getLastIndex();
            }
            status = StorageStatus.OK;
        } catch (IOException t) {
            throw new IllegalStateException("init storage failed", t);
        } finally {
            if (status != StorageStatus.OK) {
                status = StorageStatus.ERROR;
                if (storageLockChannel != null) {
                    try {
                        if (storageLock != null) {
                            storageLock.release();
                        }
                        storageLockChannel.close();
                    } catch (IOException ex) {
                        logger.error("storage release lock failed", ex);
                    }
                }
            }
        }

    }

    private void createStorageDir() throws IOException {
        Path storageDirPath = Paths.get(baseDir);
        try {
            Files.createDirectories(storageDirPath);
        } catch (FileAlreadyExistsException ex) {
            // we don't care if the dir is already exists
        }
    }

    private void recoverStorage(ManifestRecord record) throws IOException {
        Path currentFilePath = Paths.get(baseDir, FileName.getCurrentManifestFileName(storageName));
        assert Files.exists(currentFilePath);
        String currentManifestFileName = new String(Files.readAllBytes(currentFilePath), StandardCharsets.UTF_8);
        assert !Strings.isNullOrEmpty(currentManifestFileName);

        manifest.recover(currentManifestFileName);

        int logFileNumber = manifest.getLogFileNumber();
        Path baseDirPath = Paths.get(baseDir);
        List<FileName.FileNameMeta> logsFileMetas = Files.walk(baseDirPath)
                .filter(p -> (p != baseDirPath && Files.isRegularFile(p)))
                .map(p -> FileName.parseFileName(p.toString()))
                .filter(f -> f.getType() == FileName.FileType.Log && f.getFileNumber() >= logFileNumber)
                .collect(Collectors.toList());

        for (int i = 0; i < logsFileMetas.size(); ++i) {
            FileName.FileNameMeta fileMeta = logsFileMetas.get(i);
            recoverMmFromLogFiles(fileMeta.getFileNumber(), record, i == logsFileMetas.size() - 1);
        }
    }

    private void recoverMmFromLogFiles(int fileNumber, ManifestRecord record, boolean lastLogFile) throws IOException {
        Path logFilePath = Paths.get(baseDir, FileName.getLogFileName(storageName, fileNumber));
        checkState(Files.exists(logFilePath), "log file \"%s\" was deleted during recovery", logFilePath);

        long readEndPosition;
        boolean noNewSSTable = true;
        Memtable mm = null;
        try (FileChannel ch = FileChannel.open(logFilePath, StandardOpenOption.READ)) {
            LogReader reader = new LogReader(ch);
            while (true) {
                Optional<byte[]> logOpt = reader.readLog();
                if (logOpt.isPresent()) {
                    LogEntry e = LogEntry.parseFrom(logOpt.get());
                    if (mm == null) {
                        mm = new Memtable();
                    }
                    mm.add(e.getIndex(), e);
                    if (mm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                        SSTableFileMetaInfo meta = writeMemTableToSSTable(mm);
                        record.addMeta(meta);
                        noNewSSTable = false;
                        mm = null;
                    }
                } else {
                    break;
                }
            }

            readEndPosition = ch.position();
        }

        if (lastLogFile && noNewSSTable) {
            FileChannel logFile = FileChannel.open(logFilePath, StandardOpenOption.WRITE);
            assert logWriter == null;
            assert logFileNumber == 0;
            logWriter = new LogWriter(logFile, readEndPosition);
            logFileNumber = fileNumber;
            if (mm != null) {
                this.mm = mm;
                mm = null;
            }
        }

        if (mm != null) {
            SSTableFileMetaInfo meta = writeMemTableToSSTable(mm);
            record.addMeta(meta);
        }
    }

    @Override
    public synchronized int getTerm(int index) {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        if (index < getFirstIndex()) {
            throw new LogsCompactedException(index);
        }

        int lastIndex = getLastIndex();
        checkArgument(index <= lastIndex,
                "index: %s out of bound, lastIndex: %s",
                index, lastIndex);

        LogEntry e;
        if (imm != null) {
            e = imm.get(index);
            if (e != null) {
                return e.getTerm();
            }
        }

        e = mm.get(index);
        if (e != null) {
            return e.getTerm();
        }

        List<LogEntry> entries = getEntries(index, index + 1);
        if (entries.isEmpty()) {
            return -1;
        } else {
            return entries.get(0).getTerm();
        }
    }

    @Override
    public synchronized int getLastIndex() {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        assert mm.isEmpty() || mm.lastKey() == lastIndexInStorage :
            String.format("actual lastIndex:%s lastIndexInMm:%s", lastIndexInStorage, mm.lastKey());
        return lastIndexInStorage;
    }

    @Override
    public synchronized int getFirstIndex() {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);
        return firstIndexInStorage;
    }

    @Override
    public synchronized List<LogEntry> getEntries(int start, int end) {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);
        checkArgument(start < end, "end:%s should greater than start:%s", end, start);

        List<LogEntry> ret = new ArrayList<>();
        Itr itr = internalIterator(start, end);
        while (itr.hasNext()) {
            LogEntry e = itr.next();
            if (e.getIndex() >= end) {
                break;
            }

            ret.add(e);
        }

        return ret;
    }

    @Override
    public synchronized void append(List<LogEntry> entries) {
        checkNotNull(entries);
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        if (entries.isEmpty()) {
            logger.warn("append with empty entries");
            return;
        }

        if (firstIndexInStorage < 0) {
            LogEntry first = entries.get(0);
            firstIndexInStorage = first.getIndex();
        }

        try {
            for (LogEntry e : entries) {
                if (makeRoomForEntry()) {
                    byte[] data = e.toByteArray();
                    logWriter.append(data);
                    mm.add(e.getIndex(), e);
                    lastIndexInStorage = e.getIndex();
                } else {
                    throw new StorageInternalError("make room on file based storage failed");
                }
            }
        } catch (IOException ex) {
            throw new StorageInternalError("append log on file based storage failed", ex);
        }
    }

    private boolean makeRoomForEntry() {
        try {
            while (true) {
                if (status != StorageStatus.OK) {
                    return false;
                } else if (mm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                    if (imm != null) {
                        this.wait();
                        continue;
                    }

                    int nextLogFileNumber = manifest.getNextFileNumber();
                    String nextLogFile = FileName.getLogFileName(storageName, nextLogFileNumber);
                    FileChannel logFile = FileChannel.open(Paths.get(baseDir, nextLogFile),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    if (logWriter != null) {
                        logWriter.close();
                    }
                    logWriter = new LogWriter(logFile);
                    logFileNumber = nextLogFileNumber;
                    imm = mm;
                    mm = new Memtable();
                    backgroundWriteSstableRunning = true;
                    logger.debug("trigger compaction, new log file number={}", logFileNumber);
                    sstableWriterPool.submit(this::writeMemTable);
                } else {
                    return true;
                }
            }
        } catch (IOException | InterruptedException t) {
            logger.error("make room for new entry failed", t);
        }
        return false;
    }

    synchronized void waitWriteSstableFinish() throws InterruptedException {
        if (backgroundWriteSstableRunning) {
            this.wait();
        }
    }

    private void writeMemTable() {
        logger.debug("start write mem table in background");
        StorageStatus status = StorageStatus.OK;
        try {
            SSTableFileMetaInfo meta = writeMemTableToSSTable(imm);

            ManifestRecord record = ManifestRecord.newPlainRecord();
            record.addMeta(meta);
            record.setLogNumber(logFileNumber);
            manifest.logRecord(record);

            manifest.processCompactTask();

            // TODO: delete obsolete files

            logger.debug("write mem table in background done with manifest record {}", record);
        } catch (Throwable t) {
            logger.error("write memtable in background failed", t);
            status = StorageStatus.ERROR;
        } finally {
            synchronized (this) {
                backgroundWriteSstableRunning = false;
                if (status == StorageStatus.OK) {
                    imm = null;
                } else {
                    this.status = status;
                }
                this.notifyAll();
            }
        }
    }

    private SSTableFileMetaInfo writeMemTableToSSTable(Memtable mm) throws IOException {
        assert mm != null;

        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        int fileNumber = manifest.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstKey(mm.firstKey());
        meta.setLastKey(mm.lastKey());

        String tableFileName = FileName.getSSTableName(storageName, fileNumber);
        Path tableFile = Paths.get(baseDir, tableFileName);
        try (FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            TableBuilder tableBuilder = new TableBuilder(ch);
            for (LogEntry entry : mm) {
                byte[] data = entry.toByteArray();
                tableBuilder.add(entry.getIndex(), data);
            }

            long tableFileSize = tableBuilder.finishBuild();

            if (tableFileSize > 0) {
                meta.setFileSize(tableFileSize);

                ch.force(true);
            }
        }

        tableCache.loadTable(fileNumber, meta.getFileSize());

        if (meta.getFileSize() <= 0) {
            Files.deleteIfExists(tableFile);
        }

        return meta;
    }

    public CompletableFuture<Void> compact(int toIndex) {
        checkArgument(toIndex > 0);

        return manifest.compact(toIndex);
    }

    public synchronized void awaitShutdown(long timeout, TimeUnit unit) throws IOException{
        checkArgument(timeout >= 0);
        if (status == StorageStatus.SHUTTING_DOWN) {
            return;
        }

        status = StorageStatus.SHUTTING_DOWN;

        long start = System.nanoTime();
        try {
            waitWriteSstableFinish();
            sstableWriterPool.shutdown();
            timeout = unit.toNanos(timeout) - (System.nanoTime() - start);
            sstableWriterPool.awaitTermination(timeout, TimeUnit.NANOSECONDS);
        } catch (InterruptedException ex) {
            sstableWriterPool.shutdownNow();
        }

        if (logWriter != null) {
            logWriter.close();
        }

        manifest.close();

        tableCache.evictAll();

        if (storageLock != null && storageLock.isValid()) {
            storageLock.release();
        }

        if (storageLockChannel != null && storageLockChannel.isOpen()) {
            storageLockChannel.close();
        }
    }

    synchronized void shutdownNow() throws IOException {
        if (status == StorageStatus.SHUTTING_DOWN) {
            return;
        }

        status = StorageStatus.SHUTTING_DOWN;
        sstableWriterPool.shutdownNow();

        if (logWriter != null) {
            logWriter.close();
        }

        manifest.close();

        tableCache.evictAll();

        if (storageLock != null && storageLock.isValid()) {
            storageLock.release();
        }

        if (storageLockChannel != null && storageLockChannel.isOpen()) {
            storageLockChannel.close();
        }
    }

    private Itr internalIterator(int start, int end) {
        List<SeekableIterator<LogEntry>> itrs = getSSTableIterators(start, end);
        if (imm != null) {
            itrs.add(imm.iterator());
        }
        itrs.add(mm.iterator());
        for (SeekableIterator<LogEntry> itr : itrs) {
            itr.seek(start);
            if (itr.hasNext()) {
                break;
            }
        }

        return new Itr(itrs);
    }

    private List<SeekableIterator<LogEntry>> getSSTableIterators(int start, int end) {
        try {
            List<SSTableFileMetaInfo> metas = manifest.searchMetas(start, end);
            List<SeekableIterator<LogEntry>> ret = new ArrayList<>(metas.size());
            for(SSTableFileMetaInfo meta : metas) {
                ret.add(tableCache.iterator(meta.getFileNumber(), meta.getFileSize()));
            }
            return ret;
        } catch (IOException ex) {
            throw new StorageInternalError(
                    String.format("get sstable iterators start:%s end:%s from SSTable failed", start, end), ex);
        }
    }

    private class Itr implements Iterator<LogEntry> {
        private final List<SeekableIterator<LogEntry>> iterators;
        private int lastItrIndex;

        Itr(List<SeekableIterator<LogEntry>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            for (int i = lastItrIndex; i < iterators.size(); i++) {
                SeekableIterator<LogEntry> itr = iterators.get(i);
                if (itr.hasNext()) {
                    lastItrIndex = i;
                    return true;
                }
            }

            lastItrIndex = iterators.size();
            return false;
        }

        @Override
        public LogEntry next() {
            assert lastItrIndex >= 0 && lastItrIndex < iterators.size();
            return iterators.get(lastItrIndex).next();
        }
    }
}
