package raft.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.util.Strings;
import raft.server.util.ThreadFactoryImpl;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.log.StorageInternalError;
import raft.server.proto.LogEntry;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

import static raft.server.util.Preconditions.checkArgument;
import static raft.server.util.Preconditions.checkNotNull;

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
    private volatile int logFileNumber;
    private long firstIndexInStorage;
    private long lastIndexInStorage;
    private Memtable mm;
    private volatile Memtable imm;
    private volatile StorageStatus status;
    private boolean backgroundWriteSstableRunning;
    private volatile CountDownLatch shutdownLatch;

    public FileBasedStorage(String storageBaseDir, String storageName) {
        checkNotNull(storageBaseDir);
        Path baseDirPath = Paths.get(storageBaseDir);

        checkArgument(Files.notExists(baseDirPath) || Files.isDirectory(baseDirPath),
                "\"%s\" must be a directory to hold raft persistent logs", storageBaseDir);
        checkArgument(storageName.matches("[A-Za-z0-9_-]+"),
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
        checkArgument(status == StorageStatus.NEED_INIT,
                "storage don't need initialization. current status: %s", status);
        try {
            createStorageDir();

            Path lockFilePath = Paths.get(baseDir, FileName.getLockFileName(storageName));
            storageLockChannel = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            storageLock = storageLockChannel.tryLock();
            checkArgument(storageLock != null,
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
        File baseDirFile = new File(baseDir);
        File[] files = baseDirFile.listFiles();
        List<FileName.FileNameMeta> logsFileMetas = Collections.emptyList();
        if (files != null) {
            logsFileMetas = Arrays.stream(files)
                    .filter(File::isFile)
                    .map(File::getName)
                    .map(FileName::parseFileName)
                    .filter(meta -> meta.getType() == FileName.FileType.Log && meta.getFileNumber() >= logFileNumber)
                    .collect(Collectors.toList());
        }

        for (int i = 0; i < logsFileMetas.size(); ++i) {
            FileName.FileNameMeta fileMeta = logsFileMetas.get(i);
            recoverMmFromLogFiles(fileMeta.getFileNumber(), record, i == logsFileMetas.size() - 1);
        }
    }

    private void recoverMmFromLogFiles(int fileNumber, ManifestRecord record, boolean lastLogFile) throws IOException {
        Path logFilePath = Paths.get(baseDir, FileName.getLogFileName(storageName, fileNumber));
        checkArgument(Files.exists(logFilePath), "log file \"%s\" was deleted during recovery", logFilePath);

        long readEndPosition;
        boolean noNewSSTable = true;
        Memtable mm = null;
        FileChannel ch = FileChannel.open(logFilePath, StandardOpenOption.READ);
        try (LogReader reader = new LogReader(ch)) {
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
        } catch (BadRecordException ex) {
            logger.warn("got \"{}\" record in log file:\"{}\". ", ex.getType(), logFilePath);
        }

        if (mm != null) {
            SSTableFileMetaInfo meta = writeMemTableToSSTable(mm);
            record.addMeta(meta);
        }
    }

    @Override
    public synchronized long getTerm(long index) {
        checkArgument(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        if (index < getFirstIndex()) {
            throw new LogsCompactedException(index);
        }

        long lastIndex = getLastIndex();
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
    public synchronized long getLastIndex() {
        checkArgument(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        assert mm.isEmpty() || mm.lastKey() == lastIndexInStorage :
                String.format("actual lastIndex:%s lastIndexInMm:%s", lastIndexInStorage, mm.lastKey());
        return lastIndexInStorage;
    }

    @Override
    public synchronized long getFirstIndex() {
        checkArgument(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);
        return firstIndexInStorage;
    }

    @Override
    public synchronized List<LogEntry> getEntries(long start, long end) {
        checkArgument(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);
        checkArgument(start < end, "end:%s should greater than start:%s", end, start);

        if (start < getFirstIndex()) {
            throw new LogsCompactedException(start);
        }

        if (! mm.isEmpty() && start >= mm.firstKey()) {
            return mm.getEntries(start, end);
        }

        Itr itr;
        if (imm != null && start >= imm.firstKey()) {
            List<SeekableIterator<Long, LogEntry>> itrs = Arrays.asList(imm.iterator(), mm.iterator());
            for (SeekableIterator<Long, LogEntry> it : itrs) {
                it.seek(start);
            }

            itr = new Itr(itrs);
        } else {
            itr = internalIterator(start, end);
        }

        List<LogEntry> ret = new ArrayList<>();
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
    public synchronized long append(List<LogEntry> entries) {
        checkNotNull(entries);
        checkArgument(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        if (entries.isEmpty()) {
            logger.warn("append with empty entries");
            return getLastIndex();
        }

        if (firstIndexInStorage < 0) {
            LogEntry first = entries.get(0);
            firstIndexInStorage = first.getIndex();
        }

        try {
            long lastIndex = -1;
            for (LogEntry e : entries) {
                checkArgument(e.getIndex() > lastIndex,
                        "log entries being appended is not monotone increasing: %s", entries);
                lastIndex = e.getIndex();
                if (makeRoomForEntry(false)) {
                    byte[] data = e.toByteArray();
                    logWriter.append(data);
                    mm.add(e.getIndex(), e);
                    lastIndexInStorage = e.getIndex();
                } else {
                    throw new StorageInternalError("make room on file based storage failed");
                }
            }
            return getLastIndex();
        } catch (IOException ex) {
            throw new StorageInternalError("append log on file based storage failed", ex);
        }
    }

    private boolean makeRoomForEntry(boolean force) {
        try {
            boolean forceRun = force;
            while (true) {
                if (status != StorageStatus.OK) {
                    return false;
                } else if (forceRun || mm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                    if (imm != null) {
                        this.wait();
                        continue;
                    }

                    forceRun = false;
                    makeRoomForEntry0();
                } else {
                    return true;
                }
            }
        } catch (IOException | InterruptedException t) {
            logger.error("make room for new entry failed", t);
        }
        return false;
    }

    private void makeRoomForEntry0() throws IOException {
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
    }

    private void writeMemTable() {
        logger.debug("start write mem table in background");
        StorageStatus status = StorageStatus.OK;
        try {
            ManifestRecord record = null;
            if (!imm.isEmpty()) {
                if (imm.firstKey() > manifest.getLastIndex()) {
                    SSTableFileMetaInfo meta = writeMemTableToSSTable(imm);
                    record = ManifestRecord.newPlainRecord();
                    record.addMeta(meta);
                    record.setLogNumber(logFileNumber);
                } else {
                    List<SSTableFileMetaInfo> remainMetas = new ArrayList<>();

                    long firstKeyInImm = imm.firstKey();
                    List<SSTableFileMetaInfo> allMetas = manifest.searchMetas(Long.MIN_VALUE, Long.MAX_VALUE);
                    for (SSTableFileMetaInfo meta : allMetas) {
                        if (firstKeyInImm < meta.getLastKey()) {
                            remainMetas.add(mergeMemTableAndSStable(meta, imm));
                            break;
                        } else {
                            remainMetas.add(meta);
                        }
                    }

                    assert !remainMetas.isEmpty();
                    record = ManifestRecord.newReplaceAllExistedMetasRecord();
                    record.addMetas(remainMetas);
                    record.setLogNumber(logFileNumber);
                }
                manifest.logRecord(record);
            }

            if (manifest.processCompactTask()) {
                firstIndexInStorage = manifest.getFirstIndex();
            }

            Set<Integer> remainMetasFileNumberSet = manifest.searchMetas(Long.MIN_VALUE, Long.MAX_VALUE)
                    .stream()
                    .map(SSTableFileMetaInfo::getFileNumber)
                    .collect(Collectors.toSet());
            for (Integer fileNumber : tableCache.getAllFileNumbers()){
                if (!remainMetasFileNumberSet.contains(fileNumber)) {
                    tableCache.evict(fileNumber);
                }
            }

            FileName.deleteOutdatedFiles(baseDir, logFileNumber, tableCache);

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
        return mergeMemTableAndSStable(null, mm);
    }

    private SSTableFileMetaInfo mergeMemTableAndSStable(SSTableFileMetaInfo sstable, Memtable mm) throws IOException {
        assert mm != null;

        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        int fileNumber = manifest.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstKey(Math.min(mm.firstKey(), sstable != null ? sstable.getFirstKey() : Long.MAX_VALUE));
        meta.setLastKey(Math.max(mm.lastKey(), sstable != null ? sstable.getLastKey() : -1));

        String tableFileName = FileName.getSSTableName(storageName, fileNumber);
        Path tableFile = Paths.get(baseDir, tableFileName);
        try (FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE, StandardOpenOption.WRITE)) {
            TableBuilder tableBuilder = new TableBuilder(ch);

            Iterator<LogEntry> ssTableIterator = Collections.<LogEntry>emptyList().iterator();
            if (sstable != null) {
                ssTableIterator = tableCache.iterator(sstable.getFileNumber(), sstable.getFileSize());
            }

            long boundary = mm.firstKey();
            while (ssTableIterator.hasNext()) {
                LogEntry entry = ssTableIterator.next();
                if (entry.getIndex() < boundary) {
                    byte[] data = entry.toByteArray();
                    tableBuilder.add(entry.getIndex(), data);
                } else {
                    break;
                }
            }

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

    @Override
    public Future<Long> compact(long toIndex) {
        checkArgument(toIndex > 0);

        return manifest.compact(toIndex);
    }

    public synchronized void forceFlushMemtable() {
        makeRoomForEntry(true);
    }

    @Override
    public synchronized void shutdown() {
        if (status == StorageStatus.SHUTTING_DOWN) {
            return;
        }

        try {
            status = StorageStatus.SHUTTING_DOWN;
            shutdownLatch = new CountDownLatch(1);

            sstableWriterPool.submit(() -> {
                synchronized (FileBasedStorage.this) {
                    try {
                        logger.debug("shutting file based storage down");
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
                        logger.debug("file based storage shutdown successfully");
                    } catch (Exception ex) {
                        logger.error("shutdown failed", ex);
                    } finally {
                        shutdownLatch.countDown();
                    }
                }
            });
        } catch (RejectedExecutionException ex) {
            throw new StorageInternalError(ex);
        }
    }

    @Override
    public void awaitTermination() throws InterruptedException{
        for(;;) {
            if (shutdownLatch == null) {
                shutdown();
            } else {
                shutdownLatch.await();
                return;
            }
        }
    }

    synchronized void waitWriteSstableFinish() throws InterruptedException {
        if (backgroundWriteSstableRunning) {
            this.wait();
        }
    }

    private Itr internalIterator(long start, long end) {
        List<SeekableIterator<Long, LogEntry>> itrs = getSSTableIterators(start, end);
        if (imm != null) {
            itrs.add(imm.iterator());
        }
        itrs.add(mm.iterator());
        for (SeekableIterator<Long, LogEntry> itr : itrs) {
            itr.seek(start);
            if (itr.hasNext()) {
                break;
            }
        }

        return new Itr(itrs);
    }

    private List<SeekableIterator<Long, LogEntry>> getSSTableIterators(long start, long end) {
        try {
            List<SSTableFileMetaInfo> metas = manifest.searchMetas(start, end);
            List<SeekableIterator<Long, LogEntry>> ret = new ArrayList<>(metas.size());
            for (SSTableFileMetaInfo meta : metas) {
                ret.add(tableCache.iterator(meta.getFileNumber(), meta.getFileSize()));
            }
            return ret;
        } catch (IOException ex) {
            throw new StorageInternalError(
                    String.format("get sstable iterators start:%s end:%s from SSTable failed", start, end), ex);
        }
    }

    private static class Itr implements Iterator<LogEntry> {
        private final List<SeekableIterator<Long, LogEntry>> iterators;
        private int lastItrIndex;

        Itr(List<SeekableIterator<Long, LogEntry>> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            for (int i = lastItrIndex; i < iterators.size(); i++) {
                SeekableIterator<Long, LogEntry> itr = iterators.get(i);
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
