package raft.rpc.processor;

import raft.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 18/4/1
 */
public interface Processor {
    RemotingCommand processRequest(RemotingCommand request);
}
