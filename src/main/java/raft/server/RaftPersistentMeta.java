package raft.server;

import raft.server.proto.PBRaftPersistentMeta;
import raft.server.util.Strings;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.zip.CRC32;

import static raft.server.util.Preconditions.checkArgument;

/**
 * Author: ylgrgyq
 * Date: 18/4/23
 */
public class RaftPersistentMeta {
    private static final short magic = 8102;
    private static final short version = 0x01;
    private static final String fileNamePrefix = "raft_persistent_state";
    // persistent state record header length: magic, version, buffer length, and checksum
    private static final int headerLength = Short.BYTES + Short.BYTES + Integer.BYTES + Long.BYTES;

    private final Path stateFileDirPath;
    private final OpenOption[] openFileOpts;

    private String votedFor;
    private long term;
    private long commitIndex;
    private Path stateFilePath;
    private volatile boolean initialized;

    public RaftPersistentMeta(String stateFileDir, String raftId, boolean syncWriteFile) {
        checkArgument(!Strings.isNullOrEmpty(stateFileDir));
        checkArgument(!Strings.isNullOrEmpty(raftId));

        this.stateFileDirPath = Paths.get(stateFileDir);

        checkArgument(Files.notExists(stateFileDirPath) || Files.isDirectory(stateFileDirPath),
                "\"%s\" must be a directory to hold raft state file", stateFileDir);

        this.stateFilePath = Paths.get(stateFileDir + "/" + fileNamePrefix + "_" + raftId);

        if (syncWriteFile) {
            openFileOpts = new OpenOption[]{StandardOpenOption.SYNC,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE};
        } else {
            openFileOpts = new OpenOption[]{
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            };
        }
    }

    public void init() {
        if (initialized) {
            return;
        }

        if (Files.exists(stateFileDirPath)) {
            if (Files.exists(stateFilePath)) {
                try {
                    if (Files.size(stateFilePath) > 512) {
                        throw new IllegalStateException(String.format("state file: \"%s\" is too large", stateFilePath));
                    }

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

                    // TODO use byte to encode length instead of int ?
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

                    PBRaftPersistentMeta state = PBRaftPersistentMeta.parseFrom(meta);
                    term = state.getTerm();
                    votedFor = state.getVotedFor();
                    commitIndex = state.getCommitIndex();
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

        term = 0L;
        votedFor = null;
        initialized = true;
        commitIndex = -1L;
    }

    public String getVotedFor() {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        return votedFor;
    }

    public void setVotedFor(String votedFor) {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");
        checkArgument(votedFor == null || !votedFor.isEmpty(), "votedFor should not be empty string");

        this.votedFor = votedFor;
        this.persistent();
    }

    public long getTerm() {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        return term;
    }

    public void setTerm(long term) {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        this.term = term;
        this.persistent();
    }

    public void setTermAndVotedFor(long term, String votedFor) {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        this.term = term;
        this.votedFor = votedFor;
        persistent();
    }

    public long getCommitIndex() {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        checkArgument(initialized, "should initialize RaftPersistentMeta before using it");

        this.commitIndex = commitIndex;
        persistent();
    }

    public Path getStateFilePath() {
        return stateFilePath;
    }

    private void persistent() {
        PBRaftPersistentMeta.Builder builder = PBRaftPersistentMeta.newBuilder()
                .setTerm(term)
                .setCommitIndex(commitIndex);
        if (votedFor != null) {
            builder.setVotedFor(votedFor);
        }

        PBRaftPersistentMeta state = builder.build();

        byte[] meta = state.toByteArray();

        ByteBuffer buffer = ByteBuffer.allocate(headerLength + meta.length);
        buffer.putShort(magic);
        buffer.putShort(version);
        buffer.putInt(meta.length);
        buffer.put(meta);

        CRC32 checksum = new CRC32();
        checksum.update(buffer.array(), 0, buffer.position());
        buffer.putLong(checksum.getValue());
        try {
            // TODO do we need to lock state file before write to it
            Path tmpPath = Files.createTempFile(stateFileDirPath, fileNamePrefix, ".tmp_rps");
            Files.write(tmpPath, buffer.array(), openFileOpts);

            // TODO maybe we don't need to replace the existing one. we can just write a new state file with a new name
            // and leave the old state file as it is (of course, need some way to remove outdated files asynchronously).
            // On recovery, we just read the newest state file as the legal one
            Files.move(tmpPath, stateFilePath, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new PersistentStateException(ex);
        }
    }
}
