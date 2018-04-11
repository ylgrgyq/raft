package raft.server;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Author: ylgrgyq
 * Date: 18/4/11
 */
class TestUtil {
    static List<byte[]> newDataList(int count) {
        List<byte[]> dataList = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] data = new byte[5];
            ThreadLocalRandom.current().nextBytes(data);
            dataList.add(data);
        }
        return dataList;
    }
}
