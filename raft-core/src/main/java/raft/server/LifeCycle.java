package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/12/14
 */
interface LifeCycle {
    void start();

    void shutdown();
}
