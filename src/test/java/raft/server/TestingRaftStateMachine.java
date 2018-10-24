package raft.server;

import org.slf4j.Logger;
import raft.server.log.PersistentStorage;
import raft.server.proto.LogEntry;
import raft.server.proto.LogSnapshot;
import raft.server.storage.FileBasedStorage;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

class TestingRaftStateMachine implements StateMachine {
    private final BlockingQueue<LogEntry> applied = new LinkedBlockingQueue<>();
    private final Logger logger;
    private final String selfId;
    private final FileBasedStorage storage;
    private final Set<String> knownPeerIds;
    private final AtomicBoolean isLeader = new AtomicBoolean(false);
    private final AtomicBoolean isFollower = new AtomicBoolean(false);
    private final BlockingQueue<String> nodeAdded = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> nodeRemoved = new LinkedBlockingQueue<>();
    private volatile CompletableFuture<Void> waitLeaderFuture;
    private volatile CompletableFuture<Void> waitFollowerFuture;
    private volatile RaftStatusSnapshot lastStatus;
    private LogSnapshot recentSnapshot;

    TestingRaftStateMachine(Logger logger, String selfId, Collection<String> knownPeerIds, FileBasedStorage storage) {
        this.logger = logger;
        this.knownPeerIds = new HashSet<>(knownPeerIds);
        this.lastStatus = RaftStatusSnapshot.emptyStatus;
        this.selfId = selfId;
        this.storage = storage;
    }

    String getId() {
        return selfId;
    }

    RaftStatusSnapshot getLastStatus() {
        return lastStatus;
    }

    @Override
    public void onNodeAdded(RaftStatusSnapshot status, String peerId) {
        logger.info("on node added called " + peerId);
        lastStatus = status;
        knownPeerIds.add(peerId);
        nodeAdded.add(peerId);
    }

    @Override
    public void onNodeRemoved(RaftStatusSnapshot status, String peerId) {
        lastStatus = status;
        knownPeerIds.remove(peerId);
        nodeRemoved.add(peerId);
    }

    @Override
    public void onProposalCommitted(RaftStatusSnapshot status, List<LogEntry> msgs) {
        assert msgs != null && !msgs.isEmpty() : "msgs is null:" + (msgs == null);
        lastStatus = status;
        applied.addAll(msgs.stream()
                .filter(e -> !e.equals(PersistentStorage.sentinel) && !e.getData().isEmpty())
                .collect(Collectors.toList()));
    }

    @Override
    public void installSnapshot(RaftStatusSnapshot status, LogSnapshot snap) {
        lastStatus = status;
        recentSnapshot = snap;
        storage.compact(snap.getIndex());
    }

    @Override
    public Optional<LogSnapshot> getRecentSnapshot(long expectIndex) {
        if (recentSnapshot != null && recentSnapshot.getIndex() >= expectIndex) {
            return Optional.of(recentSnapshot);
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void onLeaderStart(RaftStatusSnapshot status) {
        lastStatus = status;
        isLeader.set(true);
        if (waitLeaderFuture != null) {
            waitLeaderFuture.complete(null);
        }
    }

    @Override
    public void onLeaderFinish(RaftStatusSnapshot status) {
        lastStatus = status;
        isLeader.set(false);
        waitLeaderFuture = null;
    }

    CompletableFuture<Void> becomeLeaderFuture() {
        if (isLeader.get()) {
            return CompletableFuture.completedFuture(null);
        } else {
            waitLeaderFuture = new CompletableFuture<>();
            if (isLeader.get()) {
                waitFollowerFuture = CompletableFuture.completedFuture(null);
            }
            return waitLeaderFuture;
        }
    }

    @Override
    public void onFollowerStart(RaftStatusSnapshot status) {
        // skip initial follower state
        if (status.getLeaderId() != null) {
            lastStatus = status;
            isFollower.set(true);
            if (waitFollowerFuture != null) {
                waitFollowerFuture.complete(null);
            }
        }
    }

    @Override
    public void onFollowerFinish(RaftStatusSnapshot status) {
        lastStatus = status;
        isFollower.set(false);
        waitLeaderFuture = null;
    }

    @Override
    public void onCandidateStart(RaftStatusSnapshot status) {
        lastStatus = status;
    }

    @Override
    public void onCandidateFinish(RaftStatusSnapshot status) {
        lastStatus = status;
    }

    CompletableFuture<Void> becomeFollowerFuture() {
        if (isFollower.get()) {
            return CompletableFuture.completedFuture(null);
        } else {
            waitFollowerFuture = new CompletableFuture<>();
            if (isFollower.get()) {
                waitFollowerFuture = CompletableFuture.completedFuture(null);
            }
            return waitFollowerFuture;
        }
    }

    @Override
    public void onShutdown() {
        logger.info("state machine shutdownNow");
    }

    boolean waitNodeAdded(String expectPeerId) {
        return doWaitNodeChanged(expectPeerId, nodeAdded);
    }

    boolean waitNodeRemoved(String expectPeerId) {
        return doWaitNodeChanged(expectPeerId, nodeRemoved);
    }

    private boolean doWaitNodeChanged(String expectPeerId, BlockingQueue<String> queue) {
        assert expectPeerId != null && !expectPeerId.isEmpty();

        try {
            String id = queue.poll(TestingConfigs.defaultTimeoutMs, TimeUnit.MILLISECONDS);
            return expectPeerId.equals(id);
        } catch (InterruptedException ex) {
            // ignore
        }

        return false;
    }

    List<LogEntry> waitApplied(int atLeastExpect) {
        return waitApplied(atLeastExpect, TestingConfigs.defaultTimeoutMs);
    }

    List<LogEntry> waitApplied(int atLeastExpect, long timeoutMs) {
        assert atLeastExpect >= 0 : "actual " + atLeastExpect;

        List<LogEntry> ret = new ArrayList<>(drainAvailableApplied());

        long start = System.nanoTime();
        long deadline = start + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (atLeastExpect != 0 && ret.size() < atLeastExpect && System.nanoTime() < deadline) {
            try {
                LogEntry e;
                if ((e = applied.poll(timeoutMs, TimeUnit.MILLISECONDS)) != null) {
                    ret.add(e);
                    applied.drainTo(ret);
                }
            } catch (InterruptedException ex) {
                // ignore
            }
        }

        return Collections.unmodifiableList(ret);
    }

    List<LogEntry> drainAvailableApplied() {
        List<LogEntry> ret = new ArrayList<>();

        applied.drainTo(ret);

        return Collections.unmodifiableList(ret);
    }

    CompletableFuture<LogSnapshot> waitGetSnapshot() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                while (true) {
                    if (recentSnapshot != null) {
                        return recentSnapshot;
                    }
                    Thread.sleep(1000);
                }
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
    }

    Future<Long> compact(long toIndex) {
        long term = storage.getTerm(toIndex);
        assert term != -1L;
        recentSnapshot = LogSnapshot.newBuilder()
                .setIndex(toIndex)
                .setTerm(term)
                .addAllPeerIds(knownPeerIds)
                .build();

        return storage.compact(toIndex);
    }

    void flushMemtable() {
        storage.forceFlushMemtable();
    }

    BlockingQueue<LogEntry> getApplied() {
        return applied;
    }

    public Set<String> getKnownPeerIds() {
        return Collections.unmodifiableSet(knownPeerIds);
    }
}