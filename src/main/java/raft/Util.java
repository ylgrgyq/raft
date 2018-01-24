package raft;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.SocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/30
 */
public final class Util {
    private static Logger logger = LoggerFactory.getLogger(Util.class.getName());

    private Util() {throw new UnsupportedOperationException();}

    public static String parseSocketAddress(final SocketAddress addr) {
        String remoteAddr = addr != null ? addr.toString() : "";

        if (remoteAddr.length() > 0) {
            int index = remoteAddr.lastIndexOf("/");
            if (index >= 0) {
                remoteAddr = remoteAddr.substring(index + 1);
            }
        }

        return remoteAddr;
    }

    public static String parseChannelRemoteAddr(final Channel channel) {
        if (channel == null) {
            return "";
        }

        SocketAddress addr = channel.remoteAddress();
        return Util.parseSocketAddress(addr);
    }

    public static ChannelFuture closeChannel(Channel channel) {
        final String addrRemote = Util.parseChannelRemoteAddr(channel);
        ChannelFuture f = channel.close();
        f.addListener(future ->
                logger.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess())
        );

        return f;
    }
}
