package raft.server;

import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileTime;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/4/25
 */
public class RaftPersistentStateTest {
    private static final String testingDirectory = "./target/deep/deep/deep/persistent";
    private static final Path testingDirectoryPath = Paths.get(testingDirectory);

    @Test
    public void createDirectory() throws Exception {
        Files.deleteIfExists(testingDirectoryPath);

        assertTrue(! Files.exists(testingDirectoryPath));

        RaftPersistentState pState = new RaftPersistentState(testingDirectory);
        pState.init();
        assertTrue(Files.exists(testingDirectoryPath));
        assertEquals(0, pState.getTerm());
        assertNull(pState.getVotedFor());
    }

    @Test
    public void directoryExists() throws Exception {
        if (! Files.isDirectory(testingDirectoryPath)) {
            Files.delete(testingDirectoryPath);
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
        RaftPersistentState pState = new RaftPersistentState(testingDirectory);
        Files.deleteIfExists(testingDirectoryPath);
        pState.init();

        int term = ThreadLocalRandom.current().nextInt(1);
        pState.setTerm(term);
    }

    @Test
    public void getVotedFor() throws Exception {

    }

    @Test
    public void setVotedFor() throws Exception {

    }

    @Test
    public void getTerm() throws Exception {

    }

    @Test
    public void setTerm() throws Exception {

    }

    @Test
    public void setTermAndVotedFor() throws Exception {

    }

}