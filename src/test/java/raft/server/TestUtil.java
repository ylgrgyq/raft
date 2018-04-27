package raft.server;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
class TestUtil {
    static List<byte[]> newDataList(int count) {
        List<byte[]> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] data = new byte[5];
            ThreadLocalRandom.current().nextBytes(data);
            dataList.add(data);
        }
        return dataList;
    }

    static void cleanDirectory(final Path dirPath) throws Exception{
        if (Files.isDirectory(dirPath)) {
            Stream<Path> files = Files.walk(dirPath);
            files.forEach(p -> {
                try {
                    if (p != dirPath) {
                        Files.delete(p);
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }
}
