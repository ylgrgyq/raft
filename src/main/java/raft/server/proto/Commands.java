// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

public final class Commands {
  private Commands() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_PBRaftPersistentMeta_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_PBRaftPersistentMeta_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_LogEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_LogEntry_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_LogSnapshot_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_LogSnapshot_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_ConfigChange_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_ConfigChange_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_RaftCommand_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_RaftCommand_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\016commands.proto\022\021raft.server.proto\"K\n\024P" +
      "BRaftPersistentMeta\022\014\n\004term\030\001 \001(\003\022\020\n\010vot" +
      "edFor\030\002 \001(\t\022\023\n\013commitIndex\030\003 \001(\003\"\241\001\n\010Log" +
      "Entry\022\r\n\005index\030\001 \001(\003\022\014\n\004term\030\002 \001(\003\022\014\n\004da" +
      "ta\030\003 \001(\014\0223\n\004type\030\004 \001(\0162%.raft.server.pro" +
      "to.LogEntry.EntryType\"5\n\tEntryType\022\007\n\003LO" +
      "G\020\000\022\n\n\006CONFIG\020\001\022\023\n\017TRANSFER_LEADER\020\002\"J\n\013" +
      "LogSnapshot\022\014\n\004data\030\001 \001(\014\022\r\n\005index\030\002 \001(\003" +
      "\022\014\n\004term\030\003 \001(\003\022\020\n\010peer_ids\030\004 \003(\t\"\230\001\n\014Con" +
      "figChange\022\017\n\007peer_id\030\001 \001(\t\022B\n\006action\030\002 \001",
      "(\01622.raft.server.proto.ConfigChange.Conf" +
      "igChangeAction\"3\n\022ConfigChangeAction\022\014\n\010" +
      "ADD_NODE\020\000\022\017\n\013REMOVE_NODE\020\001\"\300\005\n\013RaftComm" +
      "and\0224\n\004type\030\001 \001(\0162&.raft.server.proto.Ra" +
      "ftCommand.CmdType\022\014\n\004from\030\002 \001(\t\022\n\n\002to\030\003 " +
      "\001(\t\022\014\n\004term\030\004 \001(\003\022\026\n\016prev_log_index\030\005 \001(" +
      "\003\022\025\n\rprev_log_term\030\006 \001(\003\022\023\n\013match_index\030" +
      "\007 \001(\003\022\025\n\rleader_commit\030\010 \001(\003\022\017\n\007success\030" +
      "\t \001(\010\022,\n\007entries\030\n \003(\0132\033.raft.server.pro" +
      "to.LogEntry\022\021\n\tleader_id\030\013 \001(\t\022\026\n\016last_l",
      "og_index\030\014 \001(\003\022\025\n\rlast_log_term\030\r \001(\003\022\024\n" +
      "\014vote_granted\030\016 \001(\010\022\023\n\013leader_hint\030\017 \001(\t" +
      "\022\026\n\016force_election\030\020 \001(\010\0220\n\010snapshot\030\021 \001" +
      "(\0132\036.raft.server.proto.LogSnapshot\"\201\002\n\007C" +
      "mdType\022\010\n\004NONE\020\000\022\020\n\014REQUEST_VOTE\020\001\022\025\n\021RE" +
      "QUEST_VOTE_RESP\020\002\022\022\n\016APPEND_ENTRIES\020\003\022\027\n" +
      "\023APPEND_ENTRIES_RESP\020\004\022\016\n\nADD_SERVER\020\005\022\023" +
      "\n\017ADD_SERVER_RESP\020\006\022\021\n\rREMOVE_SERVER\020\007\022\026" +
      "\n\022REMOVE_SERVER_RESP\020\010\022\010\n\004PING\020\t\022\010\n\004PONG" +
      "\020\n\022\017\n\013TIMEOUT_NOW\020\013\022\023\n\017TRANSFER_LEADER\020\014",
      "\022\014\n\010SNAPSHOT\020\rB\002P\001b\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_raft_server_proto_PBRaftPersistentMeta_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_raft_server_proto_PBRaftPersistentMeta_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_PBRaftPersistentMeta_descriptor,
        new java.lang.String[] { "Term", "VotedFor", "CommitIndex", });
    internal_static_raft_server_proto_LogEntry_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_raft_server_proto_LogEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_LogEntry_descriptor,
        new java.lang.String[] { "Index", "Term", "Data", "Type", });
    internal_static_raft_server_proto_LogSnapshot_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_raft_server_proto_LogSnapshot_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_LogSnapshot_descriptor,
        new java.lang.String[] { "Data", "Index", "Term", "PeerIds", });
    internal_static_raft_server_proto_ConfigChange_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_raft_server_proto_ConfigChange_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_ConfigChange_descriptor,
        new java.lang.String[] { "PeerId", "Action", });
    internal_static_raft_server_proto_RaftCommand_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_raft_server_proto_RaftCommand_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_RaftCommand_descriptor,
        new java.lang.String[] { "Type", "From", "To", "Term", "PrevLogIndex", "PrevLogTerm", "MatchIndex", "LeaderCommit", "Success", "Entries", "LeaderId", "LastLogIndex", "LastLogTerm", "VoteGranted", "LeaderHint", "ForceElection", "Snapshot", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
