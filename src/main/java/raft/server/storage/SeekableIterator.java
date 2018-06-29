package raft.server.storage;

import java.util.Iterator;

/**
 * Author: ylgrgyq
 * Date: 18/6/25
 */
public interface SeekableIterator<E> extends Iterator<E>{
    void seek(int key);
}
