package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.ProposeResponse;
import raft.server.RaftServer;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
//public class ClientRequestProcessor extends AbstractProcessor<RaftClientCommand> {
//    private static final Logger logger = LoggerFactory.getLogger(ClientRequestProcessor.class.getName());
//
//    public ClientRequestProcessor(RaftServer server) {
//        super(server);
//    }
//
//    @Override
//    protected RaftClientCommand decodeRemotingCommand(byte[] requestBody) {
//        return new RaftClientCommand(requestBody);
//    }
//
//    @Override
//    protected RemotingCommand doProcess(RaftClientCommand req) {
//        logger.debug("receive client command, request={}, server={}", req, this.getServer());
//        RaftClientCommand res = new RaftClientCommand();
//        ProposeResponse ret = this.getServer().propose(req.getEntry());
//        res.setSuccess(ret.isSuccess());
//        res.setLeaderId(ret.getLeaderId());
//
//        logger.debug("respond client command, response={}, server={}", res, this.getServer());
//        return RemotingCommand.createResponseCommand(res);
//    }
//}
