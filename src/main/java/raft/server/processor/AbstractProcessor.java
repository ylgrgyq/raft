package raft.server.processor;

import raft.server.RaftServer;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
abstract class AbstractProcessor implements Processor{
    private RaftServer server;

    AbstractProcessor(RaftServer server){
        this.server = server;
    }
}
