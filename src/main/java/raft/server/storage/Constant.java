package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Constant {
    static final int kBlockSize = 32768;

    // Header is checksum (Long) + length (Short) + record type (Byte).
    static final int kHeaderSize = Long.BYTES + Short.BYTES + Byte.BYTES;

    static final int kMaxBlockSize = 4096;

    // Block Trailer is checksum(Long)
    static final int kBlockTrailerSize = Long.BYTES;

    static final long kTableMagicNumber = 24068102;

    static final int kMaxMemtableSize = 16777216;

    static final int kBlockCheckpointInterval = 64;
}
