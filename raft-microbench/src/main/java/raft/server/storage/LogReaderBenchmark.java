package raft.server.storage;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.Optional;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@State(Scope.Thread)
public class LogReaderBenchmark {
    private static final String testingDirectory = "./src/main/resources/storage";
    private static final String logFileName = "log_test";

    private LogReader2 reader;
    private byte[] original = new byte[100];

    @Setup
    public void setUp() throws Exception {
        Path p = Paths.get(testingDirectory, logFileName);
        FileChannel ch = FileChannel.open(p, StandardOpenOption.READ);
        reader = new LogReader2(ch);
        Arrays.fill(original, (byte)2);
        System.out.println("set up called");
    }

    @TearDown
    public void clean() throws Exception{

        reader.close();
        System.out.println("clean called");
    }

    @Benchmark
    public byte[] testMethod() throws Exception {
        Optional<byte[]> data = reader.readLog();
        if (data.isPresent() && data.get().length == 100) {
            if (Arrays.deepEquals(new Object[]{original}, new Object[]{data.get()})) {
                return data.get();
            }
        }

        throw new RuntimeException();
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(LogReaderBenchmark.class.getSimpleName())
                .warmupTime(TimeValue.seconds(2))
                .jvmArgs("-ea")
                .forks(1)
                .build();

        new Runner(opt).run();
    }
}
