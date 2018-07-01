package raft.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

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
    private FileLock storageLock;
    private LogWriter logWriter;
    private int firstIndexInStorage;
    private Memtable mm;
    private Memtable imm;
    private volatile StorageStatus status;
    private Future writeSstableFuture;
    private TableCache tableCache;
    private List<SSTableFileMetaInfo> metas;

    public FileBasedStorage(String storageBaseDir, String storageName) {
        checkNotNull(storageBaseDir);

        Path baseDirPath = Paths.get(storageBaseDir);

        checkArgument(Files.notExists(baseDirPath) || Files.isDirectory(baseDirPath),
                "\"%s\" must be a directory to hold raft persistent logs", storageBaseDir);

        this.mm = new Memtable();
        this.storageName = storageName;
        this.baseDir = storageBaseDir + "/" + storageName;
        this.firstIndexInStorage = -1;
        this.status = StorageStatus.NEED_INIT;
        this.tableCache = new TableCache(baseDir, storageName);
        this.metas = new ArrayList<>();
    }

    @Override
    public synchronized void init() {
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
                recoverLogFiles(logFilePath);
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

    private void recoverLogFiles(Path logFilePath) throws IOException{
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
    }

    @Override
    public synchronized int getLastIndex() {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        if (mm.isEmpty()) {
            return -1;
        } else {
            return mm.lastKey();
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

        LogEntry e = mm.get(index);
        if (e != null) {
            return e.getTerm();
        }

        if (imm != null){
            e = imm.get(index);
            if (e != null) {
                return e.getTerm();
            }
        }

        e = searchSSTable(index);
        if (e != null) {
            return e.getTerm();
        } else {
            return -1;
        }
    }

    private LogEntry searchSSTable(int key) {
        try {
            int metaIndex = findMetaIndex(key);
            if (metaIndex != -1) {
                SSTableFileMetaInfo meta = metas.get(metaIndex);
                List<LogEntry> entries = tableCache.getEntries(meta.getFileNumber(), meta.getFileSize(), key, key + 1);
                if (entries != null) {
                    return entries.get(0);
                }
            }
        } catch (IOException ex) {
            //
        }

        return null;
    }

    private int findMetaIndex(int index) {
        int start = 0;
        int end = metas.size();

        while (start < end) {
            int mid = (start + end) / 2;
            SSTableFileMetaInfo meta = metas.get(mid);
            if (index < meta.getFirstIndex()) {
                end = mid - 1;
            } else if (index > meta.getLastIndex()) {
                start = mid + 1;
            } else {
                return mid;
            }
        }

        return -1;
    }

    @Override
    public synchronized int getFirstIndex() {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);
        return firstIndexInStorage;
    }

    @Override
    public List<LogEntry> getEntries(int start, int end) {
        checkState(status == StorageStatus.OK,
                "FileBasedStorage's status is not normal, currently: %s", status);

        // TODO currently we do not support start lower than first key
        assert start >= mm.firstKey();

        return mm.getEntries(start, end);
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
                }
            } catch (IOException ex) {
                throw new RuntimeException("append log on file based storage failed", ex);
            }
        } else {
            throw new RuntimeException("make room on file based storage failed");
        }
    }

    private boolean makeRoomForEntry() {
        try {
            while (true) {
                if (status != StorageStatus.OK) {
                    return false;
                } else if (mm.getMemoryUsedInBytes() > Constant.kMaxMemtableSize) {
                    if (imm != null) {
                        writeSstableFuture.get();
                        continue;
                    }

                    int nextLogFileNumber = FileName.getNextFileNumber();
                    String nextLogFile = FileName.getLogFileName(storageName, nextLogFileNumber);
                    FileChannel logFile = FileChannel.open(Paths.get(baseDir, nextLogFile),
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND);
                    logWriter = new LogWriter(logFile);
                    imm = mm;
                    mm = new Memtable();
                    writeSstableFuture = sstableWriterPool.submit(this::writeMemTable);
                } else {
                    return true;
                }
            }
        } catch (Throwable t) {
            logger.error("make room for new entry failed", t);
        }
        return false;
    }

    private void writeMemTable() {
        try {
            SSTableFileMetaInfo meta = writeMemTableToSSTable();
            metas.add(meta);

            // TODO: write meta to manifest

            synchronized (this) {
                imm = null;

                // TODO: delete obsolete files

                writeSstableFuture = null;
            }

        } catch (Throwable t) {
            logger.error("write memtable in background failed", t);
            synchronized (this) {
                status = StorageStatus.ERROR;
            }
        }
    }

    private SSTableFileMetaInfo writeMemTableToSSTable() throws IOException{
        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        int fileNumber = FileName.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstIndex(imm.firstKey());
        meta.setLastIndex(imm.lastKey());

        String tableFileName = FileName.getSSTableName(storageName, fileNumber);
        Path tableFile = Paths.get(baseDir, tableFileName);
        try (FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {
            TableBuilder tableBuilder = new TableBuilder(ch);
            for (Map.Entry<Integer, LogEntry> entry : imm.entrySet()) {
                byte[] data = entry.getValue().toByteArray();
                tableBuilder.add(entry.getKey(), data);
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

    private class MergingItr implements SeekableIterator<LogEntry> {
        private int currentKey;
        private int currentMetaIndex;

        public MergingItr() {
            this.currentKey = getFirstIndex();
            this.currentMetaIndex = 0;
        }

        @Override
        public void seek(int key) {
            if (key > mm.firstKey()) {

            }
        }

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public LogEntry next() {
            return null;
        }
    }
}
