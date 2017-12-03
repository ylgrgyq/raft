package raft.server.processor;

import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/2
 */
public interface Processor {
    RemotingCommand processRequest(RemotingCommand request);
}
