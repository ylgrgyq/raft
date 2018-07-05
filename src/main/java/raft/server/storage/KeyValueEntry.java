package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/29
 */
class KeyValueEntry<K, V> {
    private final K key;
    private final V val;

    KeyValueEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    K getKey() {
        return key;
    }

    V getVal() {
        return val;
    }
}
