package raft.counter.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import raft.server.RaftStatusSnapshot;
import raft.server.StateMachine;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

public class CounterStateMachine implements StateMachine {
    private static final Logger logger = LoggerFactory.getLogger(CounterStateMachine.class.getSimpleName());
    private long count;

    public long getCount(){
        return count;
    }

    @Override
    public void onProposalCommitted(RaftStatusSnapshot status, List<LogEntry> msgs) {
        logger.info("on proposal committed {} {}", status, msgs.size());

        for (LogEntry msg : msgs) {
            ByteBuffer buffer = ByteBuffer.wrap(msg.getData().toByteArray());
            long inc = buffer.getLong();
            count += inc;
        }
    }

    @Override
    public void onNodeAdded(RaftStatusSnapshot status, String peerId) {

    }

    @Override
    public void onNodeRemoved(RaftStatusSnapshot status, String peerId) {

    }

    @Override
    public void onLeaderStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onLeaderFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void onFollowerStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onFollowerFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void onCandidateStart(RaftStatusSnapshot status) {

    }

    @Override
    public void onCandidateFinish(RaftStatusSnapshot status) {

    }

    @Override
    public void installSnapshot(RaftStatusSnapshot status, LogSnapshot snap) {

    }

    @Override
    public Optional<LogSnapshot> getRecentSnapshot(long expectIndex) {
        return Optional.empty();
    }

    @Override
    public void onShutdown() {

    }
}
