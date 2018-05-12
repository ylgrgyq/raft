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
    internal_static_raft_server_proto_PBRaftPersistentState_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_PBRaftPersistentState_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_raft_server_proto_LogEntry_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_raft_server_proto_LogEntry_fieldAccessorTable;
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
      "\n\016commands.proto\022\021raft.server.proto\"7\n\025P" +
      "BRaftPersistentState\022\014\n\004term\030\001 \001(\005\022\020\n\010vo" +
      "tedFor\030\002 \001(\t\"\241\001\n\010LogEntry\022\r\n\005index\030\001 \001(\005" +
      "\022\014\n\004term\030\002 \001(\005\022\014\n\004data\030\003 \001(\014\0223\n\004type\030\004 \001" +
      "(\0162%.raft.server.proto.LogEntry.EntryTyp" +
      "e\"5\n\tEntryType\022\007\n\003LOG\020\000\022\n\n\006CONFIG\020\001\022\023\n\017T" +
      "RANSFER_LEADER\020\002\"\230\001\n\014ConfigChange\022\017\n\007pee" +
      "r_id\030\001 \001(\t\022B\n\006action\030\002 \001(\01622.raft.server" +
      ".proto.ConfigChange.ConfigChangeAction\"3" +
      "\n\022ConfigChangeAction\022\014\n\010ADD_NODE\020\000\022\017\n\013RE" +
      "MOVE_NODE\020\001\"\200\005\n\013RaftCommand\0224\n\004type\030\001 \001(" +
      "\0162&.raft.server.proto.RaftCommand.CmdTyp" +
      "e\022\014\n\004from\030\002 \001(\t\022\n\n\002to\030\003 \001(\t\022\014\n\004term\030\004 \001(" +
      "\005\022\026\n\016prev_log_index\030\005 \001(\005\022\025\n\rprev_log_te" +
      "rm\030\006 \001(\005\022\023\n\013match_index\030\007 \001(\005\022\025\n\rleader_" +
      "commit\030\010 \001(\005\022\017\n\007success\030\t \001(\010\022,\n\007entries" +
      "\030\n \003(\0132\033.raft.server.proto.LogEntry\022\021\n\tl" +
      "eader_id\030\013 \001(\t\022\026\n\016last_log_index\030\014 \001(\005\022\025" +
      "\n\rlast_log_term\030\r \001(\005\022\024\n\014vote_granted\030\016 " +
      "\001(\010\022\023\n\013leader_hint\030\017 \001(\t\022\026\n\016force_electi" +
      "on\030\020 \001(\010\"\363\001\n\007CmdType\022\010\n\004NONE\020\000\022\020\n\014REQUES" +
      "T_VOTE\020\001\022\025\n\021REQUEST_VOTE_RESP\020\002\022\022\n\016APPEN" +
      "D_ENTRIES\020\003\022\027\n\023APPEND_ENTRIES_RESP\020\004\022\016\n\n" +
      "ADD_SERVER\020\005\022\023\n\017ADD_SERVER_RESP\020\006\022\021\n\rREM" +
      "OVE_SERVER\020\007\022\026\n\022REMOVE_SERVER_RESP\020\010\022\010\n\004" +
      "PING\020\t\022\010\n\004PONG\020\n\022\017\n\013TIMEOUT_NOW\020\013\022\023\n\017TRA" +
      "NSFER_LEADER\020\014B\002P\001b\006proto3"
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
    internal_static_raft_server_proto_PBRaftPersistentState_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_raft_server_proto_PBRaftPersistentState_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_PBRaftPersistentState_descriptor,
        new java.lang.String[] { "Term", "VotedFor", });
    internal_static_raft_server_proto_LogEntry_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_raft_server_proto_LogEntry_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_LogEntry_descriptor,
        new java.lang.String[] { "Index", "Term", "Data", "Type", });
    internal_static_raft_server_proto_ConfigChange_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_raft_server_proto_ConfigChange_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_ConfigChange_descriptor,
        new java.lang.String[] { "PeerId", "Action", });
    internal_static_raft_server_proto_RaftCommand_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_raft_server_proto_RaftCommand_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_raft_server_proto_RaftCommand_descriptor,
        new java.lang.String[] { "Type", "From", "To", "Term", "PrevLogIndex", "PrevLogTerm", "MatchIndex", "LeaderCommit", "Success", "Entries", "LeaderId", "LastLogIndex", "LastLogTerm", "VoteGranted", "LeaderHint", "ForceElection", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
