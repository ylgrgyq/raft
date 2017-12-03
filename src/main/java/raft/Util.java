package raft;

import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/30
 */
public class Util {
    private static Logger logger = LoggerFactory.getLogger(Util.class.getName());

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (channel == null) {
            return "";
        }

        SocketAddress addr = channel.remoteAddress();
        String remoteAddr = addr != null ? addr.toString() : "";

        if (remoteAddr.length() > 0) {
            int index = remoteAddr.lastIndexOf("/");
            if (index >= 0) {
                remoteAddr = remoteAddr.substring(index + 1);
            }
        }

        return remoteAddr;
    }

    public static void closeChannel(Channel channel) {
        final String addrRemote = Util.parseChannelRemoteAddr(channel);
        channel.close().addListener(future ->
                logger.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess())
        );
    }
}
