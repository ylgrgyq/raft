package raft.server;

import com.google.protobuf.ByteString;
import raft.server.proto.LogEntry;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
public class TestUtil {
    public static List<byte[]> newDataList(int count) {
        return newDataList(count, 100);
    }

    public static List<byte[]> newDataList(int count, int dataSize) {
        return newDataList(count, dataSize, dataSize + 1);
    }

    public static List<byte[]> newDataList(int count, int dataLowSize, int dataUpperSize) {
        List<byte[]> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            int size = ThreadLocalRandom.current().nextInt(dataLowSize, dataUpperSize);
            byte[] data = new byte[size];
            ThreadLocalRandom.current().nextBytes(data);
            dataList.add(data);
        }
        return dataList;
    }

    public static List<LogEntry> newLogEntryList(int count, int dataSize) {
       return newLogEntryList(count, dataSize, dataSize + 1);
    }

    public static List<LogEntry> newLogEntryList(int count, int dataLowSize, int dataUpperSize) {
        return newLogEntryList(count, dataLowSize, dataUpperSize, 10L, 1000L);
    }

    public static List<LogEntry> newLogEntryList(int count, int dataLowSize, int dataUpperSize, long baseTerm, long baseIndex) {
        checkArgument(count < 100000);

        List<LogEntry> ret = new ArrayList<>(count);
        List<byte[]> datas = newDataList(count, dataLowSize, dataUpperSize);
        long term = ThreadLocalRandom.current().nextLong(baseTerm, baseTerm + 1000);
        long index = ThreadLocalRandom.current().nextLong(baseIndex, baseIndex + 10000);
        double incTermRatio = (double)1/10;

        for (byte[] d : datas) {
            LogEntry entry = LogEntry.newBuilder()
                    .setData(ByteString.copyFrom(d))
                    .setTerm(term)
                    .setIndex(index)
                    .build();
            ret.add(entry);

            if (ThreadLocalRandom.current().nextDouble() < incTermRatio) {
                ++term;
            }
            ++index;
        }

        return ret;
    }

    public static <T> List<List<T>> randomPartitionList(final List<T> list) {
        List<List<T>> ret = new ArrayList<>();

        int start = 0;
        int end = ThreadLocalRandom.current().nextInt(1, 50);
        while (start < list.size()) {
            List<T> batch = list.subList(start, Math.min(end, list.size()));
            if (!batch.isEmpty()) {
                ret.add(batch);
            }

            start = end;
            end = ThreadLocalRandom.current().nextInt(start,start + 50);
        }

        return ret;
    }

    public static void cleanDirectory(final Path dirPath) {
        try {
            if (Files.isDirectory(dirPath)) {
                Stream<Path> files = Files.walk(dirPath);
                files.forEach(p -> {
                    try {
                        if (p != dirPath) {
                            if (Files.isDirectory(p)) {
                                cleanDirectory(p);
                            }
                            Files.delete(p);
                        }
                    } catch (IOException ex) {
                        throw new RuntimeException(ex);
                    }
                });
            } else {
                if (Files.notExists(dirPath)) {
                    try {
                        Files.createDirectories(dirPath);
                    } catch (FileAlreadyExistsException ex) {
                        // we don't care if the dir is already exists
                    }
                }
            }


        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
