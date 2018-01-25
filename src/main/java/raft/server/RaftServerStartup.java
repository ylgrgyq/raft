package raft.server;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Author: ylgrgyq
 * Date: 17/11/29
 */
public class RaftServerStartup {
    private static final Logger logger = LoggerFactory.getLogger(RaftServerStartup.class.getName());

    public static void main(String[] args) throws Exception {
        Options options = new Options();
        options.addOption("p", "port", true, "server port");
        options.addOption("c", "config-file", true, "config properties file path");
        options.addOption("s", "state", true, "raft server state");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);
        int serverPort = Integer.parseInt(cmd.getOptionValue("p", "6666"));
        String propertiesFile = cmd.getOptionValue("c");

        Properties prop = new Properties();
        InputStream input = null;
        try {
            if (propertiesFile == null) {
                ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
                input = classLoader.getResourceAsStream("config.properties");
            } else {
                input = new FileInputStream(propertiesFile);
            }
            prop.load(input);
        } finally {
            if (input != null) {
                input.close();
            }
        }

        List<InetSocketAddress> clientAddrs = Arrays.stream(prop.getProperty("client.addrs")
                .split(","))
                .map(addrs -> addrs.split(":"))
                .filter(addrs -> !("" + serverPort).equals(addrs[1]))
                .map(addrs -> {
                    int p = Integer.parseInt(addrs[1]);
                    return new InetSocketAddress(addrs[0], p);
                }).collect(Collectors.toList());

        RaftServer.RaftServerBuilder serverBuilder = new RaftServer.RaftServerBuilder();

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        serverBuilder.withBossGroup(bossGroup);
        serverBuilder.withWorkerGroup(workerGroup);
        serverBuilder.withServerPort(serverPort);

        if (cmd.hasOption("state")) {
            serverBuilder.withState(cmd.getOptionValue("state"));
        }

        RaftServer server = serverBuilder.build();

        try {
            server.start(clientAddrs);
            Runtime.getRuntime().addShutdownHook(new Thread(server::shutdown));
        } catch (Exception ex) {
            logger.error("start raft server failed", ex);
            server.shutdown();
        }
    }
}
