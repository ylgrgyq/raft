package raft.server;

import java.util.ArrayDeque;
import java.util.Queue;

class PeerNodeInflights {
    private int size;
    private Queue<Long> inflights;

    PeerNodeInflights(int size) {
        this.size = size;
        this.inflights = new ArrayDeque<>(size);
    }

    void addInflightIndex(long index) {
        assert !isFull();

        inflights.add(index);
    }

    void freeTo(long index) {
        while (true) {
            Long head = inflights.peek();
            if (head != null && index >= head ) {
                inflights.poll();
            } else {
                break;
            }
        }
    }

    int getSurplus(){
        return size - inflights.size();
    }

    boolean isFull() {
        return inflights.size() == size;
    }
}
