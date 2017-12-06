package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public interface SerializableCommand {
    byte[] EMPTY_BYTES = new byte[0];

    byte[] encode();

    void decode(byte[] bytes);
}
