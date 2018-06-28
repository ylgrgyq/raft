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
        checkpoints = new ArrayList<>();

        content.position(content.limit() - Integer.BYTES - checkpointSize * Integer.BYTES);
        for (int i = 0; i < checkpointSize; i++) {
            int checkpoint = content.getInt();
            checkpoints.add(checkpoint);
        }
    }

    List<byte[]> getRangeValues(int startKey, int endKey) {
        List<byte[]> ret = new ArrayList<>();

        int startCheckpoint = searchCheckpoint(startKey);
        int endCheckpoint = searchCheckpoint(endKey);

        int startOffset = checkpoints.get(startCheckpoint);
        int endOffset;
        if (endCheckpoint >= checkpoints.size()) {
            endOffset = content.limit();
        } else {
            endOffset = checkpoints.get(endCheckpoint);
        }

        for (int i = startOffset; i < endOffset; ) {
            content.position(i);
            int k = content.getInt();
            int len = content.getInt();
            if (k >= startKey && k < endKey) {
                byte[] val = readVal(i);
                ret.add(val);
            }
            i += len;
        }

        return ret;
    }

    private int searchCheckpoint(int key) {
        int start = 0;
        int end = checkpoints.size();

        while (start < end) {
            int mid = (start + end) / 2;

            int offset = checkpoints.get(mid);
            int k = readKey(offset);

            if (key < k) {
                end = mid - 1;
            } else if (key > k) {
                start = mid + 1;
            }
        }
        return start;
    }

    private int readKey(int entryStartOffset) {
        content.position(entryStartOffset);
        return content.getInt();
    }

    private byte[] readVal(int entryStartOffset) {
        content.position(entryStartOffset + Integer.BYTES);
        int len = content.getInt();
        byte[] buffer = new byte[len];
        content.get(buffer);
        return buffer;
    }
}
