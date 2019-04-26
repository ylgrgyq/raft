package raft.server;

import raft.server.proto.RaftCommand;

import java.util.ArrayList;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Author: ylgrgyq
 * Date: 18/5/12
 */
class AsyncRaftCommandBrokerProxy extends AsyncProxy implements RaftCommandBroker{
    private final RaftCommandBroker broker;

    AsyncRaftCommandBrokerProxy(RaftCommandBroker broker) {
        this.broker = Objects.requireNonNull(broker);
    }

    AsyncRaftCommandBrokerProxy(RaftCommandBroker broker, ExecutorService pool) {
        super(pool);
        this.broker = Objects.requireNonNull(broker);
    }

    @Override
    public void onWriteCommand(RaftCommand cmd) {
        notify(() -> broker.onWriteCommand(cmd));
    }

    @Override
    public void onFlushCommand() {
        notify(broker::onFlushCommand);
    }
}
