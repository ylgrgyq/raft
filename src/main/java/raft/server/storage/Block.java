package raft.server.storage;

import java.nio.ByteBuffer;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class Block {
    private ByteBuffer content;

    Block(ByteBuffer content) {
        this.content = content;
    }
}
