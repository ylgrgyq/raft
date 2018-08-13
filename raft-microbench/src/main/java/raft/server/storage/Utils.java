package raft.server.storage;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class Utils {
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
