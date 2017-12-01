package raft.server;

import io.netty.channel.Channel;

import java.net.SocketAddress;

/**
 * Author: ylgrgyq
 * Date: 17/11/30
 */
public class Util {

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
}
