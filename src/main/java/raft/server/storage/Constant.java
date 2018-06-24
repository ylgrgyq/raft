package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Constant {
    static final int kBlockSize = 32768;

    // Header is checksum (Long) + length (Short) + record type (Byte).
    static final int kHeaderSize = Long.BYTES + Short.BYTES + Byte.BYTES;

    static int kMaxBlockSize = 4096;

    // Block Trailer is checksum(Long)
    static int kBlockTrailerSize = Long.SIZE;

    static long kTableMagicNumber = 24068102;
}
