// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

public interface ConfigChangeOrBuilder extends
    // @@protoc_insertion_point(interface_extends:raft.server.proto.ConfigChange)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional string peer_id = 1;</code>
   */
  java.lang.String getPeerId();
  /**
   * <code>optional string peer_id = 1;</code>
   */
  com.google.protobuf.ByteString
      getPeerIdBytes();

  /**
   * <code>optional .raft.server.proto.ConfigChange.ConfigChangeAction action = 2;</code>
   */
  int getActionValue();
  /**
   * <code>optional .raft.server.proto.ConfigChange.ConfigChangeAction action = 2;</code>
   */
  raft.server.proto.ConfigChange.ConfigChangeAction getAction();
}
