package raft.server.storage;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/6/24
 */
class Block {
    private ByteBuffer content;
    private List<Integer> checkpoints;

    Block(ByteBuffer content) {
        this.content = content;
        content.position(content.limit() - Integer.BYTES);
        int checkpointSize = content.getInt();
        assert checkpointSize > 0;

        checkpoints = new ArrayList<>(checkpointSize);
        int checkpointStart = content.limit() - Integer.BYTES - checkpointSize * Integer.BYTES;
        content.position(checkpointStart);
        for (int i = 0; i < checkpointSize; i++) {
            int checkpoint = content.getInt();
            checkpoints.add(checkpoint);
        }
        content.rewind();
        content.limit(checkpointStart);
    }

    List<byte[]> getRangeValues(int startKey, int endKey) {
        assert startKey < endKey;

        List<byte[]> ret = new ArrayList<>();

        int startCheckpoint = findStartCheckpoint(startKey);
        int cursor = checkpoints.get(startCheckpoint);
        while (cursor < content.limit()) {
            content.position(cursor);
            int k = content.getInt();
            int len = content.getInt();
            if (k >= startKey && k < endKey) {
                byte[] val = readVal(content, len);
                ret.add(val);
            } else if (k >= endKey) {
                break;
            }
            cursor += len + Integer.BYTES + Integer.BYTES;
        }

        return ret;
    }

    private int findStartCheckpoint(int key) {
        int start = 0;
        int end = checkpoints.size();
        while (start < end - 1) {
            int mid = (start + end) / 2;

            content.position(checkpoints.get(mid));
            int k = content.getInt();

            if (key < k) {
                end = mid;
            } else if (key > k) {
                if (mid + 1 >= end) {
                    start = mid;
                } else {
                    content.position(checkpoints.get(mid + 1));
                    k = content.getInt();
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


}
