package raft.rpc;

/**
 * Author: ylgrgyq
 * Date: 17/11/22
 */
public class AppendEntriesCommand extends RaftServerCommand {
//    // index of log entry immediately preceding new ones
//    private int prevLogIndex = 0;
//    // term of prevLogIndex entry
//    private int prevLogTerm = 0;
//    // leader’s commitIndex
//    private int leaderCommit = 0;
//    private boolean success = false;
//    private List<LogEntry> entries = Collections.emptyList();
//
//    public AppendEntriesCommand(byte[] body) {
//        this.setCode(CommandCode.APPEND_ENTRIES);
//        this.decode(body);
//    }
//
//    public AppendEntriesCommand(int term, String leaderId) {
//        super(term, leaderId, CommandCode.APPEND_ENTRIES);
//    }
//
//    private List<LogEntry> decodeEntries(ByteBuffer buffer) {
//        List<LogEntry> ret;
//        int size = buffer.getInt();
//        if (size == 0) {
//            ret = Collections.emptyList();
//        } else {
//            ArrayList<LogEntry> entries = new ArrayList<>(size);
////            IntStream.range(0, size + 1).forEach(i -> LogEntry.from(buffer));
//            ret = entries;
//        }
//
//        return ret;
//    }
//
//    ByteBuffer decode(byte[] bytes) {
//        final ByteBuffer buf = super.decode(bytes);
//
//        this.prevLogIndex = buf.getInt();
//        this.prevLogTerm = buf.getInt();
//        this.leaderCommit = buf.getInt();
//        this.success = buf.get() == 1;
//
//        this.entries = this.decodeEntries(buf);
//
//        assert !buf.hasRemaining();
//        return buf;
//    }
//
//    // encode entries with entries count
//    private byte[] encodeEntries(){
//        if (this.entries.isEmpty()) {
//            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
//            buffer.putInt(0);
//            return buffer.array();
//        } else {
//            int size = this.entries.stream().mapToInt(LogEntry::getSize).sum();
//            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + size);
//            buffer.putInt(this.entries.size());
//            this.entries.stream().map(LogEntry::encode).forEach(buffer::put);
//            return buffer.array();
//        }
//    }
//
//    byte[] encode() {
//        byte[] base = super.encode();
//
//        byte[] entriesBytes = this.encodeEntries();
//
//        ByteBuffer buffer = ByteBuffer.allocate(base.length +
//                // prevLogIndex
//                Integer.BYTES +
//                // prevLogTerm
//                Integer.BYTES +
//                // leaderCommit
//                Integer.BYTES +
//                // success
//                Byte.BYTES +
//                // entries
//                entriesBytes.length);
//        buffer.put(base);
//        buffer.putInt(this.prevLogIndex);
//        buffer.putInt(this.prevLogTerm);
//        buffer.putInt(this.leaderCommit);
//        buffer.put((byte)(success ? 1 : 0));
//        buffer.put(entriesBytes);
//
//        return buffer.array();
//    }
//
//    public int getPrevLogIndex() {
//        return prevLogIndex;
//    }
//
//    public void setPrevLogIndex(int prevLogIndex) {
//        this.prevLogIndex = prevLogIndex;
//    }
//
//    public int getPrevLogTerm() {
//        return prevLogTerm;
//    }
//
//    public void setPrevLogTerm(int prevLogTerm) {
//        this.prevLogTerm = prevLogTerm;
//    }
//
//    public int getLeaderCommit() {
//        return leaderCommit;
//    }
//
//    public void setLeaderCommit(int leaderCommit) {
//        this.leaderCommit = leaderCommit;
//    }
//
//    public boolean isSuccess() {
//        return success;
//    }
//
//    public void setSuccess(boolean success) {
//        this.success = success;
//    }
//
//    public List<LogEntry> getEntries() {
//        return this.entries;
//    }
//
//    public void setEntries(List<LogEntry> entries) {
//        Preconditions.checkNotNull(entries);
//        this.entries = entries;
//    }
//
//    @Override
//    public String toString() {
//        return "AppendEntriesCommand{" +
//                "leaderId='" + this.getFrom() + '\'' +
//                ", term=" + this.getTerm() +
//                ", prevLogIndex=" + prevLogIndex +
//                ", prevLogTerm=" + prevLogTerm +
//                ", leaderCommit=" + leaderCommit +
//                ", success=" + success +
//                ", entries=" + entries +
//                '}';
//    }
}
