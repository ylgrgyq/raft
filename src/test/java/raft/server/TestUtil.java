package raft.server;

import com.google.protobuf.ByteString;
import raft.server.proto.LogEntry;

import java.io.IOException;
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
        return newDataList(count, 5);
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
        checkArgument(count < 100000);
        checkArgument(dataUpperSize < 51200);

        List<LogEntry> ret = new ArrayList<>(count);
        List<byte[]> datas = newDataList(count, dataLowSize, dataUpperSize);
        int term = ThreadLocalRandom.current().nextInt(10, 1000);
        int index = ThreadLocalRandom.current().nextInt(1000, 10000);
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

    public static List<List<LogEntry>> randomPartitionLogEntryList(final List<LogEntry> list) {
        List<List<LogEntry>> ret = new ArrayList<>();

        int start = 0;
        int end = ThreadLocalRandom.current().nextInt(list.size() + 1);
        while (start < list.size()) {
            List<LogEntry> batch = list.subList(start, end);
            if (!batch.isEmpty()) {
                ret.add(batch);
            }

            start = end;
            end = ThreadLocalRandom.current().nextInt(start,list.size() + 1);
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
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
