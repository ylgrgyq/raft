// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

public interface ConfigChangeOrBuilder extends
    // @@protoc_insertion_point(interface_extends:raft.server.proto.ConfigChange)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string peer_id = 1;</code>
   */
  java.lang.String getPeerId();
  /**
   * <code>string peer_id = 1;</code>
   */
  com.google.protobuf.ByteString
      getPeerIdBytes();

  /**
   * <code>.raft.server.proto.ConfigChange.ConfigChangeAction action = 2;</code>
   */
  int getActionValue();
  /**
   * <code>.raft.server.proto.ConfigChange.ConfigChangeAction action = 2;</code>
   */
  raft.server.proto.ConfigChange.ConfigChangeAction getAction();
}
