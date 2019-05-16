package raft.server;

/**
 * Author: ylgrgyq
 * Date: 17/12/14
 */
interface LifeCycle<S, F> {
    void start(S ctx);

    F finish();
}
