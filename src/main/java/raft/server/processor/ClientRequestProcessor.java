package raft.server.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.LogEntry;
import raft.server.RaftServer;
import raft.server.State;
import raft.server.rpc.RaftClientCommand;
import raft.server.rpc.RemotingCommand;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 17/12/26
 */
public class ClientRequestProcessor extends AbstractProcessor<RaftClientCommand> {
    private static final Logger logger = LoggerFactory.getLogger(ClientRequestProcessor.class.getName());

    public ClientRequestProcessor(RaftServer server) {
        super(server);
    }

    @Override
    protected RaftClientCommand decodeRemotingCommand(byte[] requestBody) {
        return new RaftClientCommand(requestBody);
    }

    @Override
    protected RemotingCommand doProcess(RaftClientCommand cmd) {
        RaftClientCommand res = new RaftClientCommand();
        res.setLeaderId(this.getServer().getLeaderId());
        if (this.getServer().getState() == State.LEADER) {
            LogEntry entry = cmd.getEntry();
            try {
                this.getServer().appendLog(entry);
                res.setSuccess(true);
            } catch (Throwable t) {
                logger.error("append log failed {}", entry, t);
                res.setSuccess(false);
            }
        } else {
            res.setSuccess(false);
        }

        return RemotingCommand.createResponseCommand(res);
    }
}
