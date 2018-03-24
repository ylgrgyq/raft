package raft;

import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/30
 */
public final class Util {
    private static Logger logger = LoggerFactory.getLogger(Util.class.getName());

    private Util() {throw new UnsupportedOperationException();}

    public static String parseSocketAddressToString(final SocketAddress addr) {
        String remoteAddr = addr != null ? addr.toString() : "";

        if (remoteAddr.length() > 0) {
            int index = remoteAddr.lastIndexOf("/");
            if (index >= 0) {
                remoteAddr = remoteAddr.substring(index + 1);
            }
        }

        return remoteAddr;
    }

    public static String parseChannelRemoteAddrToString(final Channel channel) {
        if (channel == null) {
            return "";
        }

        SocketAddress addr = channel.remoteAddress();
        return Util.parseSocketAddressToString(addr);
    }

    public static SocketAddress parseStringToSocketAddress(final String addr) {
        String[] s = addr.split(":");
        Preconditions.checkArgument(s.length >= 2, "failed to parse string %s to socket address", addr);
        return new InetSocketAddress(s[0], Integer.parseInt(s[1]));
    }

    public static ChannelFuture closeChannel(Channel channel) {
        final String addrRemote = Util.parseChannelRemoteAddrToString(channel);
        ChannelFuture f = channel.close();
        f.addListener(future ->
                logger.info("closeChannel: close the connection to remote address[{}] result: {}", addrRemote,
                        future.isSuccess())
        );

        return f;
    }
}
