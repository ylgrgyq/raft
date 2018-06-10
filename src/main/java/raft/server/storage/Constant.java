package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/10
 */
class Constant {
    static final int kBlockSize = 32768;

    // Header is checksum (Long), length (Short), record type (Byte).
    static final int kHeaderSize = Long.BYTES + Short.BYTES + Byte.BYTES;
}
