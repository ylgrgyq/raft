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

        Path baseDirPath = Paths.get(storageBaseDir);

        checkArgument(Files.notExists(baseDirPath) || Files.isDirectory(baseDirPath),
                "\"%s\" must be a directory to hold raft persistent logs", storageBaseDir);

        this.mm = new ConcurrentSkipListMap<>();
        this.storageName = storageName;
        this.baseDir = storageBaseDir + "/" + storageName;
        this.firstIndexInStorage = -1;
    }

    @Override
    public void init() {
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
            throw new IllegalStateException("init storage failed", t);
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

        return -1;
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

    private void writeEntriesToTable() throws IOException{
        SSTableFileMetaInfo meta = new SSTableFileMetaInfo();

        int fileNumber = FileName.getNextFileNumber();
        meta.setFileNumber(fileNumber);
        meta.setFirstIndex(imm.firstKey());
        meta.setLastIndex(imm.lastKey());

        String tableFileName = FileName.getSSTableName(storageName, fileNumber);


        Path tableFile = Paths.get(baseDir, storageName, tableFileName);
        FileChannel ch = FileChannel.open(tableFile, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        TableBuilder tableBuilder = new TableBuilder(ch);

        for (Map.Entry<Integer, LogEntry> entry : imm.entrySet()) {
            byte[] data = entry.getValue().toByteArray();
            tableBuilder.add(entry.getKey(), data);
        }

        tableBuilder.build();

        meta.setFileSize(tableBuilder.getFileSize());

        ch.force(true);

        ch.close();

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
