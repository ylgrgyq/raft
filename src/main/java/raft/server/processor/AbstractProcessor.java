package raft.server.processor;

import raft.server.RaftServer;
import raft.server.rpc.RaftCommand;
import raft.server.rpc.RaftServerCommand;
import raft.server.rpc.RemotingCommand;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
abstract class AbstractProcessor<T extends RaftCommand> implements Processor{
    private final RaftServer server;

    AbstractProcessor(RaftServer server) {
        this.server = server;
    }

    protected abstract RemotingCommand doProcess(T cmd);

    protected abstract T decodeRemotingCommand(byte[] requestBody);

    public RemotingCommand processRequest(RemotingCommand request) {
        if (request.getBody().isPresent()) {
            T cmd = this.decodeRemotingCommand(request.getBody().get());

            return this.doProcess(cmd);
        }

        // TODO send error msg to requesting peer?
        throw new RuntimeException("request body is empty");
    }

    protected RaftServer getServer() {
        return server;
    }
}
