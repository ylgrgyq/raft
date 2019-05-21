package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
public class Constant {
    public static final int kBlockSize = 32768;

    // Header is checksum (Long) + length (Short) + record type (Byte).
    public static final int kHeaderSize = Long.BYTES + Short.BYTES + Byte.BYTES;

    public static final int kMaxBlockSize = 4096;

    // Block Trailer is checksum(Long)
    public static final int kBlockTrailerSize = Long.BYTES;

    public static final long kTableMagicNumber = 24068102;

    public static final int kMaxMemtableSize = 8388608;

    public static final int kBlockCheckpointInterval = 64;
}
