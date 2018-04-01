package raft.server;

import java.util.Collections;
import java.util.List;

/**
 * Author: ylgrgyq
 * Date: 18/3/30
 */
public class Config {
    String selfId;

    long tickIntervalMs;

    List<String> peers = Collections.emptyList();
}
