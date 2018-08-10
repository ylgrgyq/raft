package raft.server.storage;

import junit.framework.AssertionFailedError;
import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class LogTest {
    private static final String testingDirectory = "./target/storage";
    private static final String logFileName = "log_test";
    private LogWriter writer;

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));

        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        writer = new LogWriter(ch);
    }

    @Test
    public void writeReadHalfBlock() throws Exception {
        List<byte[]> expectDatas = TestUtil.newDataList(1, 1, Constant.kBlockSize / 2);
        for (byte[] data : expectDatas) {
            writer.append(data);
        }

        List<byte[]> actualDatas = new ArrayList<>();
        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        try (LogReader reader = new LogReader(ch)) {
            while (true) {
                Optional<byte[]> actual = reader.readLog();
                if (actual.isPresent()) {
                    actualDatas.add(actual.get());
                } else {
                    break;
                }
            }
        }

        assertEquals(expectDatas.size(), actualDatas.size());
        for (int i = 0; i < expectDatas.size(); i++) {
            assertArrayEquals(expectDatas.get(i), actualDatas.get(i));
        }
    }

    @Test
    public void writeReadLog() throws Exception {
        List<byte[]> expectDatas = TestUtil.newDataList(10000, 1, 2 * Constant.kBlockSize);
        for (byte[] data : expectDatas) {
            writer.append(data);
        }

        List<byte[]> actualDatas = new ArrayList<>();
        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        try (LogReader reader = new LogReader(ch)) {
            while (true) {
                Optional<byte[]> actual = reader.readLog();
                if (actual.isPresent()) {
                    actualDatas.add(actual.get());
                } else {
                    break;
                }
            }
        }

        assertEquals(expectDatas.size(), actualDatas.size());
        for (int i = 0; i < expectDatas.size(); i++) {
            assertArrayEquals(expectDatas.get(i), actualDatas.get(i));
        }
    }

    @Test
    public void readUnfinishedRecord() throws Exception {
        List<byte[]> expectDatas = TestUtil.newDataList(10000, 1, 2 * Constant.kBlockSize);

        List<Long> expectDataEndPos = new ArrayList<>(expectDatas.size());
        for (byte[] data : expectDatas) {
            writer.append(data);
            expectDataEndPos.add(writer.getPosition());
        }

        int truncateDataIndex = ThreadLocalRandom.current().nextInt(100, expectDatas.size());
        long truncatePos = expectDataEndPos.get(truncateDataIndex - 1) +
                ThreadLocalRandom.current().nextInt(1, Constant.kHeaderSize);

        List<byte[]> actualDatas = new ArrayList<>();
        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.WRITE);
        ch.truncate(truncatePos);
        ch = FileChannel.open(p, StandardOpenOption.READ);
        try (LogReader reader = new LogReader(ch)) {
            while (true) {
                Optional<byte[]> actual = reader.readLog();
                if (actual.isPresent()) {
                    actualDatas.add(actual.get());
                } else {
                    break;
                }
            }
            throw new AssertionFailedError("Can't be here");
        } catch (BadRecordException ex) {
            assertEquals(RecordType.kUnfinished, ex.getType());
        }

        expectDatas = expectDatas.subList(0, truncateDataIndex);
        assertEquals(expectDatas.size(), actualDatas.size());
        for (int i = 0; i < expectDatas.size(); i++) {
            assertArrayEquals(expectDatas.get(i), actualDatas.get(i));
        }
    }
}