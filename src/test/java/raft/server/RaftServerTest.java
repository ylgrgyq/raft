package raft.server;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.connections.NettyRemoteClient;
import raft.server.rpc.AppendEntriesCommand;
import raft.server.rpc.PendingRequest;
import raft.server.rpc.RaftClientCommand;
import raft.server.rpc.RemotingCommand;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Author: ylgrgyq
 * Date: 18/1/24
 */
public class RaftServerTest {
    private static final Logger logger = LoggerFactory.getLogger(RaftServerTest.class.getName());
    private EventLoopGroup workerGroup = new NioEventLoopGroup();

//    @Before
//    public void setUp() throws Exception {
//        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
//
//        List<Integer> serverPorts = new ArrayList<>();
//        serverPorts.add(6666);
//        serverPorts.add(6667);
//        serverPorts.add(6668);
//
//        List<String> clientAddrs = serverPorts.stream().map(port -> "127.0.0.1:" + port).collect(Collectors.toList());
//
//        serverPorts.stream().map(port -> {
//            RaftServer.RaftServerBuilder serverBuilder = new RaftServer.RaftServerBuilder();
//            serverBuilder.withBossGroup(bossGroup);
//            serverBuilder.withWorkerGroup(this.workerGroup);
//            serverBuilder.withServerPort(port);
//            if (port == 6666) {
//                serverBuilder.withLeaderState();
//            }
//            try {
//                return serverBuilder.build();
//            } catch (Exception ex) {
//                logger.error("build test raft server failed", port, ex);
//                throw new RuntimeException();
//            }
//        }).forEach(server -> {
//            try {
//                server.start(clientAddrs);
//            } catch (Exception ex) {
//                logger.error("start raft server failed", ex);
//                server.shutdown();
//                throw new RuntimeException();
//            }
//        });
//    }

    @Test
    public void testSelectLeader() throws Exception {
//        Thread.sleep(5000);

        NettyRemoteClient client = new NettyRemoteClient();

        LogEntry entry = new LogEntry();
        entry.setData(new byte[]{0, 1, 2, 3, 4, 5});
        final RaftClientCommand clientReq = new RaftClientCommand();
        clientReq.setEntry(entry);

        for (int i = 0; i < 50; i++) {
            Thread.sleep(1000);
            client.send("127.0.0.1:6668", RemotingCommand.createRequestCommand(clientReq), (PendingRequest req, RemotingCommand res) -> {
                if (res.getBody().isPresent()) {
                    RaftClientCommand clientRes = new RaftClientCommand(res.getBody().get());
                    logger.info("receive response {}", clientRes);
                } else {
                    logger.error("no valid response returned for append cmd: {}. maybe request timeout", res.toString());
                }
            });
        }

        Thread.sleep(2000);
    }

    @Test
    public void setLeaderId() throws Exception {

    }

    @Test
    public void shutdown() throws Exception {

    }


}