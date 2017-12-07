package raft.server.rpc;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public interface SerializableCommand {
    byte[] EMPTY_BYTES = new byte[0];

    byte[] encode();

    ByteBuffer decode(byte[] bytes);
}
