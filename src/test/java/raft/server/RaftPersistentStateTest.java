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
    private static final String raftId = "raft 001";

    @Test
    public void createDirectory() throws Exception {
        TestUtil.cleanDirectory(testingDirectoryPath);
        Files.deleteIfExists(testingDirectoryPath);

        assertTrue(! Files.exists(testingDirectoryPath));

        RaftPersistentState pState = new RaftPersistentState(testingDirectory, raftId, false);
        pState.init();
        assertTrue(Files.exists(testingDirectoryPath));
        assertEquals(0, pState.getTerm());
        assertNull(pState.getVotedFor());
    }

    @Test
    public void emptyExistingDirectory() throws Exception {
        if (Files.isDirectory(testingDirectoryPath)) {
            TestUtil.cleanDirectory(testingDirectoryPath);
        } else {
            Files.deleteIfExists(testingDirectoryPath);
            Files.createDirectories(testingDirectoryPath);
        }

        FileTime modifiedTime = Files.getLastModifiedTime(testingDirectoryPath);

        assertTrue(Files.exists(testingDirectoryPath));

        RaftPersistentState pState = new RaftPersistentState(testingDirectory, raftId, false);
        pState.init();
        assertTrue(Files.exists(testingDirectoryPath));
        assertEquals(0, pState.getTerm());
        assertNull(pState.getVotedFor());
        assertEquals(modifiedTime.toMillis(), Files.getLastModifiedTime(testingDirectoryPath).toMillis());
    }

    @Test
    public void persistent() throws Exception {
        RaftPersistentState initState = new RaftPersistentState(testingDirectory, raftId, false);
        TestUtil.cleanDirectory(testingDirectoryPath);
        initState.init();

        final String voteFor = "Donald Trump";
        final int term = ThreadLocalRandom.current().nextInt(1, 100);
        initState.setTerm(term);
        initState.setVotedFor(voteFor);
        initState.setCommitIndex(ThreadLocalRandom.current().nextInt(1000, 10000));

        RaftPersistentState loadedState = new RaftPersistentState(testingDirectory, raftId, false);
        loadedState.init();

        assertEquals(initState.getTerm(), loadedState.getTerm());
        assertEquals(initState.getVotedFor(), loadedState.getVotedFor());
        assertEquals(initState.getCommitIndex(), loadedState.getCommitIndex());

        final String voteFor2 = "Barack Obama";
        int term2 = ThreadLocalRandom.current().nextInt(100, 200);
        loadedState.setTermAndVotedFor(term2, voteFor2);
        loadedState.setCommitIndex(ThreadLocalRandom.current().nextInt(10000, 20000));

        RaftPersistentState loadedState2 = new RaftPersistentState(testingDirectory, raftId, false);
        loadedState2.init();

        assertEquals(loadedState.getTerm(), loadedState2.getTerm());
        assertEquals(loadedState.getVotedFor(), loadedState2.getVotedFor());
        assertEquals(loadedState.getCommitIndex(), loadedState2.getCommitIndex());
    }
}