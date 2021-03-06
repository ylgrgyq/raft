package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.util.Preconditions;
import raft.server.util.ThreadFactoryImpl;

import java.util.concurrent.*;

/**
 * Author: ylgrgyq
 * Date: 18/5/12
 */
abstract class AsyncProxy {
    private static final Logger logger = LoggerFactory.getLogger(AsyncProxy.class.getName());
    private static final ThreadFactory defaultThreadFactory = new ThreadFactoryImpl("RaftAsyncProxy-");

    private static final long DEFAULT_GRACEFUL_SHUTDOWN_TIMEOUT_MILLIS = 10_000;

    private final ExecutorService pool;
    private volatile boolean unexpectedException = false;

    AsyncProxy() {
        this(Executors.newSingleThreadExecutor(defaultThreadFactory));
    }

    AsyncProxy(ExecutorService pool) {
        Preconditions.checkNotNull(pool);

        this.pool = pool;
    }

    CompletableFuture<Void> notify(Runnable job) {
        if (!unexpectedException) {
            return CompletableFuture
                    .runAsync(job, pool)
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            logger.error("notify failed, will not accept any new notification job afterward", ex);
                            unexpectedException = true;
                        }
                    });
        } else {
            throw new IllegalStateException("proxy shutdown due to unexpected exception, please check log to debug");
        }
    }

    CompletableFuture<Void> shutdown() {
        return CompletableFuture
                .runAsync(pool::shutdown, pool);
    }
}
