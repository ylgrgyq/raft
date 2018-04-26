package raft.server;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.proto.PBRaftPersistentState;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.zip.CRC32;

/**
 * Author: ylgrgyq
 * Date: 18/4/23
 */
public class RaftPersistentState {
    private static final Logger logger = LoggerFactory.getLogger(RaftPersistentState.class.getName());

    private static final short magic = 8102;
    private static final short version = 0x01;
    private static final String fileName = "raft_persistent_state";

    private String votedFor;
    // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    // TODO need persistent
    private int term;
    private Path stateFileDirPath;
    private Path stateFilePath;
    private volatile boolean initialized;

    public RaftPersistentState(String stateFileDir) {
        Preconditions.checkArgument(! Strings.isNullOrEmpty(stateFileDir));

        this.stateFileDirPath = Paths.get(stateFileDir);

        Preconditions.checkArgument(Files.notExists(stateFileDirPath) || Files.isDirectory(stateFileDirPath),
                "\"%s\" must be a directory to hold raft state file", stateFileDir);

        this.stateFilePath = Paths.get(stateFileDir + "/" + fileName);
    }

    public void init() {
        if (initialized) {
            return;
        }

        if (Files.exists(stateFileDirPath)) {
            if (Files.exists(stateFilePath)) {
                try {
                    byte[] raw = Files.readAllBytes(stateFilePath);
                    ByteBuffer buffer = ByteBuffer.wrap(raw);
                    if (magic != buffer.getShort()) {
                        String msg = String.format("unrecognized persistent state file: \"%s\"", stateFilePath);
                        throw new IllegalStateException(msg);
                    }

                    short verInFile = buffer.getShort();
                    if (version != verInFile) {
                        String msg = String.format("can't read raft persistent state from a file with different " +
                                "version, version in file: %s, permit version: %s", verInFile, version);
                        throw new IllegalStateException(msg);
                    }

                    int length = buffer.getInt();
                    byte[] meta = new byte[length];
                    buffer.get(meta);

                    CRC32 actualChecksum = new CRC32();
                    actualChecksum.update(raw, 0, raw.length - Long.BYTES);
                    long expectChecksum = buffer.getLong();
                    if (expectChecksum != actualChecksum.getValue()) {
                        String msg = String.format("broken raft persistent file: \"%s\"", stateFilePath);
                        throw new IllegalStateException(msg);
                    }

                    PBRaftPersistentState state = PBRaftPersistentState.parseFrom(meta);
                    term = state.getTerm();
                    votedFor = state.getVotedFor();
                    initialized = true;
                    return;
                } catch (BufferUnderflowException | IOException ex) {
                    String msg = String.format("invalid raft persistent state file: \"%s\"", stateFilePath);
                    throw new IllegalStateException(msg, ex);
                }
            }
        } else {
            try {
                Files.createDirectories(stateFileDirPath);
            } catch (IOException ex) {
                String msg = String.format("can't create directory for path: \"%s\"", stateFileDirPath);
                throw new IllegalStateException(msg, ex);
            }
        }

        term = 0;
        votedFor = null;
        initialized = true;
    }

    public String getVotedFor() {
        Preconditions.checkState(initialized, "should initialize RaftPersistentState before using it");

        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        Preconditions.checkState(initialized, "should initialize RaftPersistentState before using it");
        Preconditions.checkArgument(votedFor == null || ! votedFor.isEmpty(), "votedFor should not be empty string");

        this.votedFor = votedFor;
        this.persistent();
    }

    public int getTerm() {
        Preconditions.checkState(initialized, "should initialize RaftPersistentState before using it");

        return term;
    }

    public void setTerm(int term) {
        Preconditions.checkState(initialized, "should initialize RaftPersistentState before using it");

        this.term = term;
        this.persistent();
    }

    public void setTermAndVotedFor(int term, String votedFor) {
        Preconditions.checkState(initialized, "should initialize RaftPersistentState before using it");

        this.term = term;
        this.votedFor = votedFor;
        this.persistent();
    }

    private void persistent() {
        PBRaftPersistentState.Builder builder = PBRaftPersistentState.newBuilder().setTerm(term);
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }

        PBRaftPersistentState state = builder.build();

        byte[] meta = state.toByteArray();

        // allocate a buffer for magic, version, buffer length, serialized state and checksum
        ByteBuffer buffer = ByteBuffer.allocate(Short.BYTES + Short.BYTES + Integer.BYTES + meta.length + Long.BYTES);
        buffer.putShort(magic);
        buffer.putShort(version);
        buffer.putInt(meta.length);
        buffer.put(meta);

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());
        buffer.putLong(checksum.getValue());
        try {
            Path tmpPath = Files.createTempFile(stateFileDirPath, fileName, ".tmp_rps");
            Files.write(tmpPath, buffer.array());

            Files.move(tmpPath, stateFilePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new PersistentStateException(ex);
        }
    }
}
