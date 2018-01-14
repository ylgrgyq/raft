package raft.server;

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.RemoteClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.PendingRequestCallback;
import raft.server.rpc.RemotingCommand;

/**
 * Author: ylgrgyq
 * Date: 18/1/12
 */
public class RaftPeerNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeerNode.class.getName());

    private RemoteClient remoteClient;
    private RaftServer server;

    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private int matchIndex;

    RaftPeerNode(RemoteClient remote, int nextIndex) {
        this.remoteClient = remote;
        this.nextIndex = nextIndex;
        this.matchIndex = 1;
    }

    void sendAppend(AppendEntriesCommand base) {

        RemotingCommand cmd = RemotingCommand.createRequestCommand(base);
        remoteClient.sendOneway(cmd).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                logger.warn("ping to {} failed", remoteClient, f.cause());
                remoteClient.close();
            }
        });
    }

    Future<Void> send(RemotingCommand cmd, PendingRequestCallback callback) {
        return this.remoteClient.send(cmd, callback);
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
        if (matchIndex >= this.nextIndex) {
            this.nextIndex = matchIndex + 1;
        }
    }
}
