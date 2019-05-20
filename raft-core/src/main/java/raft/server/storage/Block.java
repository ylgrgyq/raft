package raft.server.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class Block implements Iterable<KeyValueEntry<Long, byte[]>>{
    private final ByteBuffer content;
    private final List<Integer> checkpoints;

    Block(ByteBuffer content) {
        this.content = content;
        content.position(content.limit() - Integer.BYTES);
        int checkpointSize = content.getInt();
        assert checkpointSize > 0;

        checkpoints = new ArrayList<>(checkpointSize);
        int checkpointStart = content.limit() - Integer.BYTES - checkpointSize * Integer.BYTES;
        content.position(checkpointStart);
        int lastCheckpoint = -1;
        for (int i = 0; i < checkpointSize; i++) {
            int checkpoint = content.getInt();
            assert lastCheckpoint < checkpoint :
                    String.format("checkpoint:%s, lastCheckpoint:%s", checkpoint, lastCheckpoint);
            lastCheckpoint = checkpoint;
            assert checkpoint < checkpointStart :
                    String.format("checkpoint:%s, checkpointStart:%s", checkpoint, checkpointStart);
            checkpoints.add(checkpoint);
        }
        content.rewind();
        content.limit(checkpointStart);
    }

    List<byte[]> getValuesByKeyRange(long startKey, long endKey) {
        assert startKey < endKey;

        List<byte[]> ret = new ArrayList<>();

        SeekableIterator<Long, KeyValueEntry<Long, byte[]>> iter = iterator();
        iter.seek(startKey);

        while (iter.hasNext()) {
            KeyValueEntry<Long, byte[]> entry = iter.next();
            if (entry.getKey() < endKey) {
                ret.add(entry.getVal());
            } else {
                break;
            }
        }

        return ret;
    }

    private int findStartCheckpoint(long key) {
        int start = 0;
        int end = checkpoints.size();
        while (start < end - 1) {
            int mid = (start + end) / 2;

            content.position(checkpoints.get(mid));
            long k = content.getLong();

            if (key < k) {
                end = mid;
            } else if (key > k) {
                if (mid + 1 >= end) {
                    start = mid;
                } else {
                    content.position(checkpoints.get(mid + 1));
                    k = content.getLong();
                    if (key > k) {
                        start = mid + 1;
                    } else {
                        start = mid;
                        break;
                    }
                }
            } else {
                break;
            }
        }

        return start;
    }

    private byte[] readVal(ByteBuffer src, int len) {
        byte[] buffer = new byte[len];
        src.get(buffer);
        return buffer;
    }

    @Override
    public SeekableIterator<Long, KeyValueEntry<Long, byte[]>> iterator() {
        return new Itr(content);
    }

    private class Itr implements SeekableIterator<Long, KeyValueEntry<Long, byte[]>> {
        private final ByteBuffer content;
        private int offset;

        Itr(ByteBuffer content) {
            this.content = content;
        }

        @Override
        public void seek(Long key) {
            int checkpoint = findStartCheckpoint(key);
            offset = checkpoints.get(checkpoint);
            assert offset < content.limit();
            while (offset < content.limit()) {
                content.position(offset);
                long k = content.getLong();
                int len = content.getInt();
                assert len > 0;
                if (k < key) {
                    offset += len + Long.BYTES + Integer.BYTES;
                } else {
                    break;
                }
            }
        }

        @Override
        public boolean hasNext() {
            return offset < content.limit();
        }

        @Override
        public KeyValueEntry<Long, byte[]> next() {
            assert offset < content.limit();

            content.position(offset);
            long k = content.getLong();
            int len = content.getInt();
            assert len > 0;
            byte[] val = readVal(content, len);

            offset += len + Long.BYTES + Integer.BYTES;

            return new KeyValueEntry<>(k, val);
        }
    }
}
