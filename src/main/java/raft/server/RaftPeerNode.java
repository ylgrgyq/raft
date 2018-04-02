package raft.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.log.RaftLog;

/**
 * Author: ylgrgyq
 * Date: 18/1/12
 */
class RaftPeerNode {
    private static final Logger logger = LoggerFactory.getLogger(RaftPeerNode.class.getName());

    private final String peerId;
    private final RaftServer server;
    private final RaftLog serverLog;

    // index of the next log entry to send to that server (initialized to leader last log index + 1)
    private int nextIndex;
    // index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
    private int matchIndex;

    RaftPeerNode(String peerId, RaftServer server, RaftLog log, int nextIndex) {
        this.peerId = peerId;
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
        this.server = server;
        this.serverLog = log;
    }

    synchronized void sendAppend(int maxMsgSize) {
//        final int startIndex = this.nextIndex;
//        final List<LogEntry> entries = serverLog.getEntries(startIndex - 1, startIndex + maxMsgSize);
//
//        // entries must not empty even for heartbeat
//        // TODO use a dedicated heartbeat command? so we do not need to send prev log/term in heartbeat
//        assert !entries.isEmpty();
//
//        final AppendEntriesCommand appendReq = new AppendEntriesCommand(this.server.getTerm(), this.server.getLeaderId());
//        appendReq.setLeaderCommit(serverLog.getCommitIndex());
//
//        final LogEntry prev = entries.get(0);
//        appendReq.setPrevLogTerm(prev.getTerm());
//        appendReq.setPrevLogIndex(prev.getIndex());
//        appendReq.setEntries(entries.subList(1, entries.size()));
//
//        logger.debug("send directAppend {}", appendReq);
//        this.send(RemotingCommand.createRequestCommand(appendReq),
//                (PendingRequest req, RemotingCommand res) -> {
//                    if (res.getBody().isPresent()) {
//                        final AppendEntriesCommand appendRes = new AppendEntriesCommand(res.getBody().get());
//                        if (appendRes.getTerm() > this.server.getTerm()) {
//                            this.server.tryBecomeFollower(appendRes.getTerm(), appendRes.getFrom());
//                        } else {
//                            synchronized (RaftPeerNode.this) {
//                                if (appendRes.isSuccess()) {
//                                    this.matchIndex = entries.get(entries.size() - 1).getIndex();
//                                    this.nextIndex = this.matchIndex + 1;
//                                    this.server.updateCommit();
//                                } else {
//                                    this.nextIndex--;
//                                    if (this.nextIndex < 1) {
//                                        logger.warn("nextIndex for {} decreased to 1", this.toString());
//                                        this.nextIndex = 1;
//                                    }
//                                    assert this.nextIndex > this.matchIndex;
//                                    this.sendAppend(maxMsgSize);
//                                }
//                            }
//                        }
//                    } else {
//                        logger.error("no valid response returned for directAppend cmd: {}. maybe request timeout", appendReq.toString());
//                    }
//                });
    }

    synchronized void reset(int nextIndex) {
        this.nextIndex = nextIndex;
        this.matchIndex = 0;
    }

    int getMatchIndex() {
        return matchIndex;
    }

    public String getPeerId() {
        return peerId;
    }

    @Override
    public String toString() {
        return "RaftPeerNode{" +
                "peerId='" + peerId + '\'' +
                ", nextIndex=" + nextIndex +
                ", matchIndex=" + matchIndex +
                '}';
    }
}
