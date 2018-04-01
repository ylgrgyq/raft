package raft.server.processor;

import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface Processor {
    RemotingCommand processRequest(RemotingCommand request);
}
