// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

public interface PBRaftPersistentStateOrBuilder extends
    // @@protoc_insertion_point(interface_extends:raft.server.proto.PBRaftPersistentState)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>int32 term = 1;</code>
   */
  int getTerm();

  /**
   * <code>string votedFor = 2;</code>
   */
  java.lang.String getVotedFor();
  /**
   * <code>string votedFor = 2;</code>
   */
  com.google.protobuf.ByteString
      getVotedForBytes();
}
