package raft.server.storage;

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
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

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

            Path lockFilePath = Paths.get(baseDir, storageName);
            FileChannel ch = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            storageLock = ch.tryLock();
            checkState(storageLock != null,
                    "file storage: \"%s\" is occupied by other process", baseDir);

            Path logFilePath = Paths.get(baseDir, "log0.log");
            if (Files.exists(logFilePath)) {
                checkState(Files.isRegularFile(logFilePath),
                        "%s is not a regular file",
                        logFilePath);
                recoverMmFromLogFiles(logFilePath);
            } else {
                FileChannel c = FileChannel.open(logFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                logWriter = new LogWriter(c);
            }
        } catch (IOException t) {
            status = StorageStatus.ERROR;
            throw new IllegalStateException("init storage failed", t);
        }
        status = StorageStatus.OK;
    }

    private void createStorageDir() throws IOException {
        Path storageDirPath = Paths.get(baseDir);
        try {
            Files.createDirectories(storageDirPath);
        } catch (FileAlreadyExistsException ex) {
            // we don't care if the dir is already exists
        }
    }

    private void recoverMmFromLogFiles(Path logFilePath) throws IOException{
        FileChannel ch = FileChannel.open(logFilePath, StandardOpenOption.READ);
        LogReader reader = new LogReader(ch);

        while (true) {
            Optional<byte[]> logOpt = reader.readLog();
            if (logOpt.isPresent()) {
                LogEntry e = LogEntry.parseFrom(logOpt.get());
                mm.add(e.getIndex(), e);
            } else {
                break;
            }
        }

        if (firstIndexInStorage < 0) {
            firstIndexInStorage = mm.firstKey();
        }

        if (lastIndexInStorage < 0) {
            lastIndexInStorage = mm.lastKey();
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

        assert !mm.isEmpty() && mm.lastKey() == lastIndexInStorage;
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

        if (makeRoomForEntry()) {
            try {
                for (LogEntry e : entries) {
                    byte[] data = e.toByteArray();
                    logWriter.append(data);
                    mm.add(e.getIndex(), e);
                    lastIndexInStorage = e.getIndex();
                }
            } catch (IOException ex) {
                throw new StorageInternalError("append log on file based storage failed", ex);
            }
        } else {
            throw new StorageInternalError("make room on file based storage failed");
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
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
                    logWriter = new LogWriter(logFile);
                    logFileNumber = nextLogFileNumber;
                    imm = mm;
                    mm = new Memtable();
                    backgroundWriteSstableRunning = true;
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

    synchronized void waitWriteSstableFinish() throws InterruptedException{
        if (backgroundWriteSstableRunning) {
            this.wait();
        }
    }

    private void writeMemTable() {
        StorageStatus status = StorageStatus.OK;
        try {
            SSTableFileMetaInfo meta = writeMemTableToSSTable();
            manifest.registerMeta(meta);

            ManifestRecord record = new ManifestRecord();
            record.addMeta(meta);
            record.setLogNumber(logFileNumber);
            manifest.logRecord(record);

            // TODO: delete obsolete files

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

    private SSTableFileMetaInfo writeMemTableToSSTable() throws IOException{
        assert imm != null;

        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        int fileNumber = manifest.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstKey(imm.firstKey());
        meta.setLastKey(imm.lastKey());

        String tableFileName = FileName.getSSTableName(storageName, fileNumber);
        Path tableFile = Paths.get(baseDir, tableFileName);
        try (FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {
            TableBuilder tableBuilder = new TableBuilder(ch);
            for (LogEntry entry : imm) {
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

    public void compact(int toIndex) {

    }

    public void destroyStorage() {
        try {
            storageLock.release();
        } catch (IOException ex) {
            //
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
        List<SeekableIterator<LogEntry>> ret = new ArrayList<>();
        try {
            Iterator<SSTableFileMetaInfo> itr = manifest.searchMetas(start, end);
            while (itr.hasNext()) {
                SSTableFileMetaInfo meta = itr.next();
                ret.add(tableCache.iterator(meta.getFileNumber(), meta.getFileSize()));
            }
        } catch (IOException ex) {
            throw new StorageInternalError(
                    String.format("get sstable iterators start:%s end:%s from SSTable failed", start, end), ex);
        }

        return ret;
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
