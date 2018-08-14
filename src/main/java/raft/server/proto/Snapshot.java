// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

/**
 * Protobuf type {@code raft.server.proto.Snapshot}
 */
public  final class Snapshot extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:raft.server.proto.Snapshot)
    SnapshotOrBuilder {
  // Use Snapshot.newBuilder() to construct.
  private Snapshot(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private Snapshot() {
    data_ = com.google.protobuf.ByteString.EMPTY;
    index_ = 0;
    term_ = 0;
    peerIds_ = com.google.protobuf.LazyStringArrayList.EMPTY;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private Snapshot(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    this();
    int mutable_bitField0_ = 0;
    try {
      boolean done = false;
      while (!done) {
        int tag = input.readTag();
        switch (tag) {
          case 0:
            done = true;
            break;
          default: {
            if (!input.skipField(tag)) {
              done = true;
            }
            break;
          }
          case 10: {

            data_ = input.readBytes();
            break;
          }
          case 16: {

            index_ = input.readInt32();
            break;
          }
          case 24: {

            term_ = input.readInt32();
            break;
          }
          case 34: {
            java.lang.String s = input.readStringRequireUtf8();
            if (!((mutable_bitField0_ & 0x00000008) == 0x00000008)) {
              peerIds_ = new com.google.protobuf.LazyStringArrayList();
              mutable_bitField0_ |= 0x00000008;
            }
            peerIds_.add(s);
            break;
          }
        }
      }
    } catch (com.google.protobuf.InvalidProtocolBufferException e) {
      throw e.setUnfinishedMessage(this);
    } catch (java.io.IOException e) {
      throw new com.google.protobuf.InvalidProtocolBufferException(
          e).setUnfinishedMessage(this);
    } finally {
      if (((mutable_bitField0_ & 0x00000008) == 0x00000008)) {
        peerIds_ = peerIds_.getUnmodifiableView();
      }
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return raft.server.proto.Commands.internal_static_raft_server_proto_Snapshot_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return raft.server.proto.Commands.internal_static_raft_server_proto_Snapshot_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            raft.server.proto.Snapshot.class, raft.server.proto.Snapshot.Builder.class);
  }

  private int bitField0_;
  public static final int DATA_FIELD_NUMBER = 1;
  private com.google.protobuf.ByteString data_;
  /**
   * <code>optional bytes data = 1;</code>
   */
  public com.google.protobuf.ByteString getData() {
    return data_;
  }

  public static final int INDEX_FIELD_NUMBER = 2;
  private int index_;
  /**
   * <code>optional int32 index = 2;</code>
   */
  public int getIndex() {
    return index_;
  }

  public static final int TERM_FIELD_NUMBER = 3;
  private int term_;
  /**
   * <code>optional int32 term = 3;</code>
   */
  public int getTerm() {
    return term_;
  }

  public static final int PEER_IDS_FIELD_NUMBER = 4;
  private com.google.protobuf.LazyStringList peerIds_;
  /**
   * <code>repeated string peer_ids = 4;</code>
   */
  public com.google.protobuf.ProtocolStringList
      getPeerIdsList() {
    return peerIds_;
  }
  /**
   * <code>repeated string peer_ids = 4;</code>
   */
  public int getPeerIdsCount() {
    return peerIds_.size();
  }
  /**
   * <code>repeated string peer_ids = 4;</code>
   */
  public java.lang.String getPeerIds(int index) {
    return peerIds_.get(index);
  }
  /**
   * <code>repeated string peer_ids = 4;</code>
   */
  public com.google.protobuf.ByteString
      getPeerIdsBytes(int index) {
    return peerIds_.getByteString(index);
  }

  private byte memoizedIsInitialized = -1;
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!data_.isEmpty()) {
      output.writeBytes(1, data_);
    }
    if (index_ != 0) {
      output.writeInt32(2, index_);
    }
    if (term_ != 0) {
      output.writeInt32(3, term_);
    }
    for (int i = 0; i < peerIds_.size(); i++) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 4, peerIds_.getRaw(i));
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!data_.isEmpty()) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(1, data_);
    }
    if (index_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(2, index_);
    }
    if (term_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(3, term_);
    }
    {
      int dataSize = 0;
      for (int i = 0; i < peerIds_.size(); i++) {
        dataSize += computeStringSizeNoTag(peerIds_.getRaw(i));
      }
      size += dataSize;
      size += 1 * getPeerIdsList().size();
    }
    memoizedSize = size;
    return size;
  }

  private static final long serialVersionUID = 0L;
  @java.lang.Override
  public boolean equals(final java.lang.Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof raft.server.proto.Snapshot)) {
      return super.equals(obj);
    }
    raft.server.proto.Snapshot other = (raft.server.proto.Snapshot) obj;

    boolean result = true;
    result = result && getData()
        .equals(other.getData());
    result = result && (getIndex()
        == other.getIndex());
    result = result && (getTerm()
        == other.getTerm());
    result = result && getPeerIdsList()
        .equals(other.getPeerIdsList());
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + DATA_FIELD_NUMBER;
    hash = (53 * hash) + getData().hashCode();
    hash = (37 * hash) + INDEX_FIELD_NUMBER;
    hash = (53 * hash) + getIndex();
    hash = (37 * hash) + TERM_FIELD_NUMBER;
    hash = (53 * hash) + getTerm();
    if (getPeerIdsCount() > 0) {
      hash = (37 * hash) + PEER_IDS_FIELD_NUMBER;
      hash = (53 * hash) + getPeerIdsList().hashCode();
    }
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static raft.server.proto.Snapshot parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static raft.server.proto.Snapshot parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static raft.server.proto.Snapshot parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static raft.server.proto.Snapshot parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static raft.server.proto.Snapshot parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static raft.server.proto.Snapshot parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static raft.server.proto.Snapshot parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static raft.server.proto.Snapshot parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static raft.server.proto.Snapshot parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static raft.server.proto.Snapshot parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(raft.server.proto.Snapshot prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @java.lang.Override
  protected Builder newBuilderForType(
      com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code raft.server.proto.Snapshot}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:raft.server.proto.Snapshot)
      raft.server.proto.SnapshotOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_Snapshot_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_Snapshot_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              raft.server.proto.Snapshot.class, raft.server.proto.Snapshot.Builder.class);
    }

    // Construct using raft.server.proto.Snapshot.newBuilder()
    private Builder() {
      maybeForceBuilderInitialization();
    }

    private Builder(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      super(parent);
      maybeForceBuilderInitialization();
    }
    private void maybeForceBuilderInitialization() {
      if (com.google.protobuf.GeneratedMessageV3
              .alwaysUseFieldBuilders) {
      }
    }
    public Builder clear() {
      super.clear();
      data_ = com.google.protobuf.ByteString.EMPTY;

      index_ = 0;

      term_ = 0;

      peerIds_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000008);
      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_Snapshot_descriptor;
    }

    public raft.server.proto.Snapshot getDefaultInstanceForType() {
      return raft.server.proto.Snapshot.getDefaultInstance();
    }

    public raft.server.proto.Snapshot build() {
      raft.server.proto.Snapshot result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public raft.server.proto.Snapshot buildPartial() {
      raft.server.proto.Snapshot result = new raft.server.proto.Snapshot(this);
      int from_bitField0_ = bitField0_;
      int to_bitField0_ = 0;
      result.data_ = data_;
      result.index_ = index_;
      result.term_ = term_;
      if (((bitField0_ & 0x00000008) == 0x00000008)) {
        peerIds_ = peerIds_.getUnmodifiableView();
        bitField0_ = (bitField0_ & ~0x00000008);
      }
      result.peerIds_ = peerIds_;
      result.bitField0_ = to_bitField0_;
      onBuilt();
      return result;
    }

    public Builder clone() {
      return (Builder) super.clone();
    }
    public Builder setField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.setField(field, value);
    }
    public Builder clearField(
        com.google.protobuf.Descriptors.FieldDescriptor field) {
      return (Builder) super.clearField(field);
    }
    public Builder clearOneof(
        com.google.protobuf.Descriptors.OneofDescriptor oneof) {
      return (Builder) super.clearOneof(oneof);
    }
    public Builder setRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        int index, Object value) {
      return (Builder) super.setRepeatedField(field, index, value);
    }
    public Builder addRepeatedField(
        com.google.protobuf.Descriptors.FieldDescriptor field,
        Object value) {
      return (Builder) super.addRepeatedField(field, value);
    }
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof raft.server.proto.Snapshot) {
        return mergeFrom((raft.server.proto.Snapshot)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(raft.server.proto.Snapshot other) {
      if (other == raft.server.proto.Snapshot.getDefaultInstance()) return this;
      if (other.getData() != com.google.protobuf.ByteString.EMPTY) {
        setData(other.getData());
      }
      if (other.getIndex() != 0) {
        setIndex(other.getIndex());
      }
      if (other.getTerm() != 0) {
        setTerm(other.getTerm());
      }
      if (!other.peerIds_.isEmpty()) {
        if (peerIds_.isEmpty()) {
          peerIds_ = other.peerIds_;
          bitField0_ = (bitField0_ & ~0x00000008);
        } else {
          ensurePeerIdsIsMutable();
          peerIds_.addAll(other.peerIds_);
        }
        onChanged();
      }
      onChanged();
      return this;
    }

    public final boolean isInitialized() {
      return true;
    }

    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      raft.server.proto.Snapshot parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (raft.server.proto.Snapshot) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }
    private int bitField0_;

    private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes data = 1;</code>
     */
    public com.google.protobuf.ByteString getData() {
      return data_;
    }
    /**
     * <code>optional bytes data = 1;</code>
     */
    public Builder setData(com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  
      data_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional bytes data = 1;</code>
     */
    public Builder clearData() {
      
      data_ = getDefaultInstance().getData();
      onChanged();
      return this;
    }

    private int index_ ;
    /**
     * <code>optional int32 index = 2;</code>
     */
    public int getIndex() {
      return index_;
    }
    /**
     * <code>optional int32 index = 2;</code>
     */
    public Builder setIndex(int value) {
      
      index_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 index = 2;</code>
     */
    public Builder clearIndex() {
      
      index_ = 0;
      onChanged();
      return this;
    }

    private int term_ ;
    /**
     * <code>optional int32 term = 3;</code>
     */
    public int getTerm() {
      return term_;
    }
    /**
     * <code>optional int32 term = 3;</code>
     */
    public Builder setTerm(int value) {
      
      term_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int32 term = 3;</code>
     */
    public Builder clearTerm() {
      
      term_ = 0;
      onChanged();
      return this;
    }

    private com.google.protobuf.LazyStringList peerIds_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    private void ensurePeerIdsIsMutable() {
      if (!((bitField0_ & 0x00000008) == 0x00000008)) {
        peerIds_ = new com.google.protobuf.LazyStringArrayList(peerIds_);
        bitField0_ |= 0x00000008;
       }
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public com.google.protobuf.ProtocolStringList
        getPeerIdsList() {
      return peerIds_.getUnmodifiableView();
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public int getPeerIdsCount() {
      return peerIds_.size();
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public java.lang.String getPeerIds(int index) {
      return peerIds_.get(index);
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public com.google.protobuf.ByteString
        getPeerIdsBytes(int index) {
      return peerIds_.getByteString(index);
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public Builder setPeerIds(
        int index, java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensurePeerIdsIsMutable();
      peerIds_.set(index, value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public Builder addPeerIds(
        java.lang.String value) {
      if (value == null) {
    throw new NullPointerException();
  }
  ensurePeerIdsIsMutable();
      peerIds_.add(value);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public Builder addAllPeerIds(
        java.lang.Iterable<java.lang.String> values) {
      ensurePeerIdsIsMutable();
      com.google.protobuf.AbstractMessageLite.Builder.addAll(
          values, peerIds_);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public Builder clearPeerIds() {
      peerIds_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bitField0_ = (bitField0_ & ~0x00000008);
      onChanged();
      return this;
    }
    /**
     * <code>repeated string peer_ids = 4;</code>
     */
    public Builder addPeerIdsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) {
    throw new NullPointerException();
  }
  checkByteStringIsUtf8(value);
      ensurePeerIdsIsMutable();
      peerIds_.add(value);
      onChanged();
      return this;
    }
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }

    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return this;
    }


    // @@protoc_insertion_point(builder_scope:raft.server.proto.Snapshot)
  }

  // @@protoc_insertion_point(class_scope:raft.server.proto.Snapshot)
  private static final raft.server.proto.Snapshot DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new raft.server.proto.Snapshot();
  }

  public static raft.server.proto.Snapshot getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<Snapshot>
      PARSER = new com.google.protobuf.AbstractParser<Snapshot>() {
    public Snapshot parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new Snapshot(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<Snapshot> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<Snapshot> getParserForType() {
    return PARSER;
  }

  public raft.server.proto.Snapshot getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

