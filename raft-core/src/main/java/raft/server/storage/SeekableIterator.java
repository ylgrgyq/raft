package raft.server.storage;

import java.util.Iterator;

/**
 * Author: ylgrgyq
 * Date: 18/6/25
 */
interface SeekableIterator<K, E> extends Iterator<E>{
    void seek(K key);
}
