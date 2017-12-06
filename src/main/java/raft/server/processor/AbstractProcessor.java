package raft.server.processor;

import raft.server.RaftServer;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
abstract class AbstractProcessor implements Processor{
    protected RaftServer server;

    AbstractProcessor(RaftServer server){
        this.server = server;
    }

    protected RaftServer getServer() {
        return server;
    }
}
