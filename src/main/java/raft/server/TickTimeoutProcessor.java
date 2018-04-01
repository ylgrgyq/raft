package raft.server;

/**
 * Author: ylgrgyq
 * Date: 18/1/21
 */
interface TickTimeoutProcessor {
    boolean isTickTimeout(long currentTick);

    void onTickTimeout();
}
