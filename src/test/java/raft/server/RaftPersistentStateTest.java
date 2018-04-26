package raft.server;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/25
 */
public class RaftPersistentStateTest {
    private static final String testingDirectory = "./target/deep/deep/deep/persistent";
    private static final Path testingDirectoryPath = Paths.get(testingDirectory);

    private static void cleanDirectory(final Path dirPath) throws Exception{
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

    @Test
    public void createDirectory() throws Exception {
        cleanDirectory(testingDirectoryPath);
        Files.deleteIfExists(testingDirectoryPath);

        assertTrue(! Files.exists(testingDirectoryPath));

        RaftPersistentState pState = new RaftPersistentState(testingDirectory);
        pState.init();
        assertTrue(Files.exists(testingDirectoryPath));
        assertEquals(0, pState.getTerm());
        assertNull(pState.getVotedFor());
    }

    @Test
    public void emptyExistingDirectory() throws Exception {
        if (Files.isDirectory(testingDirectoryPath)) {
            cleanDirectory(testingDirectoryPath);
        } else {
            Files.deleteIfExists(testingDirectoryPath);
            Files.createDirectories(testingDirectoryPath);
        }

        FileTime modifiedTime = Files.getLastModifiedTime(testingDirectoryPath);

        assertTrue(Files.exists(testingDirectoryPath));

        RaftPersistentState pState = new RaftPersistentState(testingDirectory);
        pState.init();
        assertTrue(Files.exists(testingDirectoryPath));
        assertEquals(0, pState.getTerm());
        assertNull(pState.getVotedFor());
        assertEquals(modifiedTime.toMillis(), Files.getLastModifiedTime(testingDirectoryPath).toMillis());
    }

    @Test
    public void persistent() throws Exception {
        RaftPersistentState initState = new RaftPersistentState(testingDirectory);
        cleanDirectory(testingDirectoryPath);
        initState.init();

        final String voteFor = "Donald Trump";
        final int term = ThreadLocalRandom.current().nextInt(1, 100);
        initState.setTerm(term);
        initState.setVotedFor(voteFor);

        RaftPersistentState loadedState = new RaftPersistentState(testingDirectory);
        loadedState.init();

        assertEquals(initState.getTerm(), loadedState.getTerm());
        assertEquals(initState.getVotedFor(), loadedState.getVotedFor());

        final String voteFor2 = "Barack Obama";
        int term2 = ThreadLocalRandom.current().nextInt(100, 200);
        loadedState.setTermAndVotedFor(term2, voteFor2);

        RaftPersistentState loadedState2 = new RaftPersistentState(testingDirectory);
        loadedState2.init();

        assertEquals(loadedState.getTerm(), loadedState2.getTerm());
        assertEquals(loadedState.getVotedFor(), loadedState2.getVotedFor());
    }
}