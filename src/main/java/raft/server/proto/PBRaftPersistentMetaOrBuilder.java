// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

public interface PBRaftPersistentMetaOrBuilder extends
    // @@protoc_insertion_point(interface_extends:raft.server.proto.PBRaftPersistentMeta)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional int32 term = 1;</code>
   */
  int getTerm();

  /**
   * <code>optional string votedFor = 2;</code>
   */
  java.lang.String getVotedFor();
  /**
   * <code>optional string votedFor = 2;</code>
   */
  com.google.protobuf.ByteString
      getVotedForBytes();

  /**
   * <code>optional int32 commitIndex = 3;</code>
   */
  int getCommitIndex();
}