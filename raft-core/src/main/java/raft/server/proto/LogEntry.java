// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: commands.proto

package raft.server.proto;

/**
 * Protobuf type {@code raft.server.proto.LogEntry}
 */
public  final class LogEntry extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:raft.server.proto.LogEntry)
    LogEntryOrBuilder {
  // Use LogEntry.newBuilder() to construct.
  private LogEntry(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private LogEntry() {
    index_ = 0L;
    term_ = 0L;
    data_ = com.google.protobuf.ByteString.EMPTY;
    type_ = 0;
  }

  @java.lang.Override
  public final com.google.protobuf.UnknownFieldSet
  getUnknownFields() {
    return com.google.protobuf.UnknownFieldSet.getDefaultInstance();
  }
  private LogEntry(
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
          case 8: {

            index_ = input.readInt64();
            break;
          }
          case 16: {

            term_ = input.readInt64();
            break;
          }
          case 26: {

            data_ = input.readBytes();
            break;
          }
          case 32: {
            int rawValue = input.readEnum();

            type_ = rawValue;
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
      makeExtensionsImmutable();
    }
  }
  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return raft.server.proto.Commands.internal_static_raft_server_proto_LogEntry_descriptor;
  }

  protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internalGetFieldAccessorTable() {
    return raft.server.proto.Commands.internal_static_raft_server_proto_LogEntry_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            raft.server.proto.LogEntry.class, raft.server.proto.LogEntry.Builder.class);
  }

  /**
   * Protobuf enum {@code raft.server.proto.LogEntry.EntryType}
   */
  public enum EntryType
      implements com.google.protobuf.ProtocolMessageEnum {
    /**
     * <code>LOG = 0;</code>
     */
    LOG(0),
    /**
     * <code>CONFIG = 1;</code>
     */
    CONFIG(1),
    /**
     * <code>TRANSFER_LEADER = 2;</code>
     */
    TRANSFER_LEADER(2),
    UNRECOGNIZED(-1),
    ;

    /**
     * <code>LOG = 0;</code>
     */
    public static final int LOG_VALUE = 0;
    /**
     * <code>CONFIG = 1;</code>
     */
    public static final int CONFIG_VALUE = 1;
    /**
     * <code>TRANSFER_LEADER = 2;</code>
     */
    public static final int TRANSFER_LEADER_VALUE = 2;


    public final int getNumber() {
      if (this == UNRECOGNIZED) {
        throw new java.lang.IllegalArgumentException(
            "Can't get the number of an unknown enum value.");
      }
      return value;
    }

    /**
     * @deprecated Use {@link #forNumber(int)} instead.
     */
    @java.lang.Deprecated
    public static EntryType valueOf(int value) {
      return forNumber(value);
    }

    public static EntryType forNumber(int value) {
      switch (value) {
        case 0: return LOG;
        case 1: return CONFIG;
        case 2: return TRANSFER_LEADER;
        default: return null;
      }
    }

    public static com.google.protobuf.Internal.EnumLiteMap<EntryType>
        internalGetValueMap() {
      return internalValueMap;
    }
    private static final com.google.protobuf.Internal.EnumLiteMap<
        EntryType> internalValueMap =
          new com.google.protobuf.Internal.EnumLiteMap<EntryType>() {
            public EntryType findValueByNumber(int number) {
              return EntryType.forNumber(number);
            }
          };

    public final com.google.protobuf.Descriptors.EnumValueDescriptor
        getValueDescriptor() {
      return getDescriptor().getValues().get(ordinal());
    }
    public final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptorForType() {
      return getDescriptor();
    }
    public static final com.google.protobuf.Descriptors.EnumDescriptor
        getDescriptor() {
      return raft.server.proto.LogEntry.getDescriptor().getEnumTypes().get(0);
    }

    private static final EntryType[] VALUES = values();

    public static EntryType valueOf(
        com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
      if (desc.getType() != getDescriptor()) {
        throw new java.lang.IllegalArgumentException(
          "EnumValueDescriptor is not for this type.");
      }
      if (desc.getIndex() == -1) {
        return UNRECOGNIZED;
      }
      return VALUES[desc.getIndex()];
    }

    private final int value;

    private EntryType(int value) {
      this.value = value;
    }

    // @@protoc_insertion_point(enum_scope:raft.server.proto.LogEntry.EntryType)
  }

  public static final int INDEX_FIELD_NUMBER = 1;
  private long index_;
  /**
   * <code>optional int64 index = 1;</code>
   */
  public long getIndex() {
    return index_;
  }

  public static final int TERM_FIELD_NUMBER = 2;
  private long term_;
  /**
   * <code>optional int64 term = 2;</code>
   */
  public long getTerm() {
    return term_;
  }

  public static final int DATA_FIELD_NUMBER = 3;
  private com.google.protobuf.ByteString data_;
  /**
   * <code>optional bytes data = 3;</code>
   */
  public com.google.protobuf.ByteString getData() {
    return data_;
  }

  public static final int TYPE_FIELD_NUMBER = 4;
  private int type_;
  /**
   * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
   */
  public int getTypeValue() {
    return type_;
  }
  /**
   * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
   */
  public raft.server.proto.LogEntry.EntryType getType() {
    raft.server.proto.LogEntry.EntryType result = raft.server.proto.LogEntry.EntryType.valueOf(type_);
    return result == null ? raft.server.proto.LogEntry.EntryType.UNRECOGNIZED : result;
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
    if (index_ != 0L) {
      output.writeInt64(1, index_);
    }
    if (term_ != 0L) {
      output.writeInt64(2, term_);
    }
    if (!data_.isEmpty()) {
      output.writeBytes(3, data_);
    }
    if (type_ != raft.server.proto.LogEntry.EntryType.LOG.getNumber()) {
      output.writeEnum(4, type_);
    }
  }

  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (index_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(1, index_);
    }
    if (term_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(2, term_);
    }
    if (!data_.isEmpty()) {
      size += com.google.protobuf.CodedOutputStream
        .computeBytesSize(3, data_);
    }
    if (type_ != raft.server.proto.LogEntry.EntryType.LOG.getNumber()) {
      size += com.google.protobuf.CodedOutputStream
        .computeEnumSize(4, type_);
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
    if (!(obj instanceof raft.server.proto.LogEntry)) {
      return super.equals(obj);
    }
    raft.server.proto.LogEntry other = (raft.server.proto.LogEntry) obj;

    boolean result = true;
    result = result && (getIndex()
        == other.getIndex());
    result = result && (getTerm()
        == other.getTerm());
    result = result && getData()
        .equals(other.getData());
    result = result && type_ == other.type_;
    return result;
  }

  @java.lang.Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptorForType().hashCode();
    hash = (37 * hash) + INDEX_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getIndex());
    hash = (37 * hash) + TERM_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getTerm());
    hash = (37 * hash) + DATA_FIELD_NUMBER;
    hash = (53 * hash) + getData().hashCode();
    hash = (37 * hash) + TYPE_FIELD_NUMBER;
    hash = (53 * hash) + type_;
    hash = (29 * hash) + unknownFields.hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static raft.server.proto.LogEntry parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static raft.server.proto.LogEntry parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static raft.server.proto.LogEntry parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static raft.server.proto.LogEntry parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static raft.server.proto.LogEntry parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static raft.server.proto.LogEntry parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static raft.server.proto.LogEntry parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static raft.server.proto.LogEntry parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static raft.server.proto.LogEntry parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static raft.server.proto.LogEntry parseFrom(
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
  public static Builder newBuilder(raft.server.proto.LogEntry prototype) {
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
   * Protobuf type {@code raft.server.proto.LogEntry}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:raft.server.proto.LogEntry)
      raft.server.proto.LogEntryOrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_LogEntry_descriptor;
    }

    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_LogEntry_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              raft.server.proto.LogEntry.class, raft.server.proto.LogEntry.Builder.class);
    }

    // Construct using raft.server.proto.LogEntry.newBuilder()
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
      index_ = 0L;

      term_ = 0L;

      data_ = com.google.protobuf.ByteString.EMPTY;

      type_ = 0;

      return this;
    }

    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return raft.server.proto.Commands.internal_static_raft_server_proto_LogEntry_descriptor;
    }

    public raft.server.proto.LogEntry getDefaultInstanceForType() {
      return raft.server.proto.LogEntry.getDefaultInstance();
    }

    public raft.server.proto.LogEntry build() {
      raft.server.proto.LogEntry result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    public raft.server.proto.LogEntry buildPartial() {
      raft.server.proto.LogEntry result = new raft.server.proto.LogEntry(this);
      result.index_ = index_;
      result.term_ = term_;
      result.data_ = data_;
      result.type_ = type_;
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
      if (other instanceof raft.server.proto.LogEntry) {
        return mergeFrom((raft.server.proto.LogEntry)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(raft.server.proto.LogEntry other) {
      if (other == raft.server.proto.LogEntry.getDefaultInstance()) return this;
      if (other.getIndex() != 0L) {
        setIndex(other.getIndex());
      }
      if (other.getTerm() != 0L) {
        setTerm(other.getTerm());
      }
      if (other.getData() != com.google.protobuf.ByteString.EMPTY) {
        setData(other.getData());
      }
      if (other.type_ != 0) {
        setTypeValue(other.getTypeValue());
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
      raft.server.proto.LogEntry parsedMessage = null;
      try {
        parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        parsedMessage = (raft.server.proto.LogEntry) e.getUnfinishedMessage();
        throw e.unwrapIOException();
      } finally {
        if (parsedMessage != null) {
          mergeFrom(parsedMessage);
        }
      }
      return this;
    }

    private long index_ ;
    /**
     * <code>optional int64 index = 1;</code>
     */
    public long getIndex() {
      return index_;
    }
    /**
     * <code>optional int64 index = 1;</code>
     */
    public Builder setIndex(long value) {
      
      index_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 index = 1;</code>
     */
    public Builder clearIndex() {
      
      index_ = 0L;
      onChanged();
      return this;
    }

    private long term_ ;
    /**
     * <code>optional int64 term = 2;</code>
     */
    public long getTerm() {
      return term_;
    }
    /**
     * <code>optional int64 term = 2;</code>
     */
    public Builder setTerm(long value) {
      
      term_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional int64 term = 2;</code>
     */
    public Builder clearTerm() {
      
      term_ = 0L;
      onChanged();
      return this;
    }

    private com.google.protobuf.ByteString data_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>optional bytes data = 3;</code>
     */
    public com.google.protobuf.ByteString getData() {
      return data_;
    }
    /**
     * <code>optional bytes data = 3;</code>
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
     * <code>optional bytes data = 3;</code>
     */
    public Builder clearData() {
      
      data_ = getDefaultInstance().getData();
      onChanged();
      return this;
    }

    private int type_ = 0;
    /**
     * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
     */
    public int getTypeValue() {
      return type_;
    }
    /**
     * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
     */
    public Builder setTypeValue(int value) {
      type_ = value;
      onChanged();
      return this;
    }
    /**
     * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
     */
    public raft.server.proto.LogEntry.EntryType getType() {
      raft.server.proto.LogEntry.EntryType result = raft.server.proto.LogEntry.EntryType.valueOf(type_);
      return result == null ? raft.server.proto.LogEntry.EntryType.UNRECOGNIZED : result;
    }
    /**
     * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
     */
    public Builder setType(raft.server.proto.LogEntry.EntryType value) {
      if (value == null) {
        throw new NullPointerException();
      }
      
      type_ = value.getNumber();
      onChanged();
      return this;
    }
    /**
     * <code>optional .raft.server.proto.LogEntry.EntryType type = 4;</code>
     */
    public Builder clearType() {
      
      type_ = 0;
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


    // @@protoc_insertion_point(builder_scope:raft.server.proto.LogEntry)
  }

  // @@protoc_insertion_point(class_scope:raft.server.proto.LogEntry)
  private static final raft.server.proto.LogEntry DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new raft.server.proto.LogEntry();
  }

  public static raft.server.proto.LogEntry getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<LogEntry>
      PARSER = new com.google.protobuf.AbstractParser<LogEntry>() {
    public LogEntry parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
        return new LogEntry(input, extensionRegistry);
    }
  };

  public static com.google.protobuf.Parser<LogEntry> parser() {
    return PARSER;
  }

  @java.lang.Override
  public com.google.protobuf.Parser<LogEntry> getParserForType() {
    return PARSER;
  }

  public raft.server.proto.LogEntry getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

