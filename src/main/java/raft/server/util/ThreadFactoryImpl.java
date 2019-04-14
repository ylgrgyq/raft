package raft.server.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Author: ylgrgyq
 * Date: 17/12/13
 */
public class ThreadFactoryImpl implements ThreadFactory {
    private final AtomicLong threadIndex = new AtomicLong(0);
    private final String threadNamePrefix;

    public ThreadFactoryImpl(final String threadNamePrefix) {
        if (threadNamePrefix.endsWith("-")){
            this.threadNamePrefix = threadNamePrefix;
        } else {
            this.threadNamePrefix  = threadNamePrefix + "-";
        }
    }

    @Override
    public Thread newThread(Runnable r) {
        return new Thread(r, threadNamePrefix + this.threadIndex.incrementAndGet());

    }
}

