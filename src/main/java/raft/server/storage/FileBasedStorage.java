package raft.server.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.LogsCompactedException;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.google.common.base.Preconditions.*;

/**
 * Author: ylgrgyq
 * Date: 18/6/8
 */
public class FileBasedStorage implements PersistentStorage {
    private static final Logger logger = LoggerFactory.getLogger(FileBasedStorage.class.getName());

    private final String baseDir;
    private final String storageName;
    private FileLock storageLock;
    private LogWriter logWriter;
    private int firstIndexInStorage;
    private ConcurrentSkipListMap<Integer, LogEntry> mm;
    private ConcurrentSkipListMap<Integer, LogEntry> imm;

    public FileBasedStorage(String storageBaseDir, String storageName) {
        checkNotNull(storageBaseDir);

        this.baseDir = storageBaseDir;
        Path baseDirPath = Paths.get(storageBaseDir);

        checkArgument(Files.notExists(baseDirPath) || Files.isDirectory(baseDirPath),
                "\"%s\" must be a directory to hold raft persistent logs", storageBaseDir);

        this.mm = new ConcurrentSkipListMap<>();
        this.storageName = storageName;
        this.firstIndexInStorage = -1;
    }

    @Override
    public void init() {
        try {
            createStorageDir();

            Path lockFilePath = Paths.get(baseDir, storageName, storageName);
            FileChannel ch = FileChannel.open(lockFilePath, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            storageLock = ch.tryLock();
            checkState(storageLock != null,
                    "file storage: \"%s\" under path: \"%s\" is occupied by other process",
                    storageName, baseDir);

            Path logFilePath = Paths.get(baseDir, storageName, "log0.log");
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
            throw new IllegalStateException("init storage failed", t);
        }
    }

    private void createStorageDir() throws IOException {
        Path storageDirPath = Paths.get(baseDir, storageName);
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
                mm.put(e.getIndex(), e);
            } else {
                break;
            }
        }

        if (firstIndexInStorage < 0) {
            firstIndexInStorage = mm.firstKey();
        }
    }

    @Override
    public int getLastIndex() {
        if (mm.isEmpty()) {
            return -1;
        } else {
            return mm.lastKey();
        }
    }

    @Override
    public int getTerm(int index) {
        if (index < getFirstIndex()) {
            throw new LogsCompactedException(index);
        }

        int lastIndex = getLastIndex();
        checkArgument(index <= lastIndex,
                "index: %s out of bound, lastIndex: %s",
                index, lastIndex);

        return mm.get(index).getTerm();
    }

    @Override
    public int getFirstIndex() {
        return firstIndexInStorage;
    }

    @Override
    public List<LogEntry> getEntries(int start, int end) {
        // TODO currently we do not support start lower than first key
        assert start >= mm.firstKey();

        ConcurrentNavigableMap<Integer, LogEntry> ents = mm.subMap(start, end);
        List<LogEntry> entries = new ArrayList<>(ents.size());
        for (Map.Entry<Integer, LogEntry> e: ents.entrySet()) {
            entries.add(e.getValue());
        }

        return entries;
    }

    @Override
    public void append(List<LogEntry> entries) {
        checkNotNull(entries);
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
                    mm.put(e.getIndex(), e);
                }
            } catch (IOException ex) {
                throw new RuntimeException("append log on file based storage failed", ex);
            }
        } else {
            throw new RuntimeException("make room on file based storage failed");
        }
    }

    boolean makeRoomForEntry() {


        return true;
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
}
