package raft.server.storage;

import org.junit.Before;
import org.junit.Test;
import raft.server.TestUtil;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

public class LogTest {
    private static final String testingDirectory = "./target/storage";
    private static final String logFileName = "log_test";
    private LogWriter writer;

    @Before
    public void setUp() throws Exception {
        TestUtil.cleanDirectory(Paths.get(testingDirectory));

        BlockBuilder builder = new BlockBuilder();

        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
        writer = new LogWriter(ch);
    }
    @Test
    public void writeReadLog() throws Exception {
        byte[] expectData = new byte[]{1, 2, 3, 4};
        writer.append(expectData);

        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        LogReader reader = new LogReader(ch);
        Optional<byte[]> actualData = reader.readLog();

        assertTrue(actualData.isPresent());
        assertArrayEquals(expectData, actualData.get());
    }
}