package raft.server.storage;

/**
 * Author: ylgrgyq
 * Date: 18/6/29
 */
class KeyValueEntry<K, V> {
    private final K key;
    private final V val;

    public KeyValueEntry(K key, V val) {
        this.key = key;
        this.val = val;
    }

    public K getKey() {
        return key;
    }

    public V getVal() {
        return val;
    }
}
