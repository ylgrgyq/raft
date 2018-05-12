package raft.server;

import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.ThreadFactoryImpl;
import raft.server.proto.RaftCommand;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Author: ylgrgyq
 * Date: 18/5/12
 */
public class AsyncRaftCommandBrokerProxy implements RaftCommandBroker{
    private static final Logger logger = LoggerFactory.getLogger(AsyncRaftCommandBrokerProxy.class.getName());

    private final ExecutorService notifier;
    private final RaftCommandBroker broker;
    private volatile boolean gotUnexpectedException = false;

    AsyncRaftCommandBrokerProxy(RaftCommandBroker broker) {
        this(broker, Executors.newSingleThreadExecutor(
                new ThreadFactoryImpl("RaftCommandBrokerProxy-")));
    }

    AsyncRaftCommandBrokerProxy(RaftCommandBroker broker, ExecutorService notifier) {
        Preconditions.checkNotNull(broker);
        Preconditions.checkNotNull(notifier);

        this.notifier = notifier;
        this.broker = broker;
    }

    private CompletableFuture<Void> notify(Runnable job) {
        if (!gotUnexpectedException) {
            return CompletableFuture
                    .runAsync(job, notifier)
                    .whenComplete((r, ex) -> {
                        if (ex != null) {
                            logger.error("notify raft command broker failed," +
                                    " will not accept any new notification job afterward", ex);
                            gotUnexpectedException = true;
                        }
                    });
        } else {
            throw new RuntimeException("RaftCommandBroker shutdown due to unexpected exception, please check log to debug");
        }
    }

    @Override
    public void onWriteCommand(RaftCommand cmd) {
        notify(() -> broker.onWriteCommand(cmd));
    }

    void shutdown() {
        notifier.shutdown();
    }
}
