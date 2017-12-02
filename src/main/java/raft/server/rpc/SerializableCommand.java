package raft.server.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public interface SerializableCommand {
    byte[] encode();

    void decode(byte[] bytes);
}
