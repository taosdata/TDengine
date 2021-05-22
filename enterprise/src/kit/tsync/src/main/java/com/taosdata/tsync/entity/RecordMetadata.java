package com.taosdata.tsync.entity;

public class RecordMetadata {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long timestamp;
    private final long serializedValueSize;
    private  volatile Long checksum;

    public RecordMetadata(String topic, int partition, long offset, long timestamp, long serializedValueSize) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.timestamp = timestamp;
        this.serializedValueSize = serializedValueSize;
    }

    public String topic() {
        return this.topic;
    }

    public int partition() {
        return this.partition;
    }

    public boolean hasOffset() {
        // Long.MIN_VALUE is NULL in TDengine
        return this.offset != Long.MIN_VALUE;
    }

    public long offset() {
        return this.offset;
    }

    public boolean hasTimestamp() {
        // Long.MIN_VALUE is NULL in TDengine
        return this.timestamp != Long.MIN_VALUE;
    }

    public long timestamp() {
        return this.timestamp;
    }

    public long checksum() {
        return this.checksum;
    }

    public long serializedValueSize() {
        return this.serializedValueSize;
    }

    @Override
    public String toString() {
        return "RecordMetadata{" +
                "topic='" + topic + '\'' +
                ", partition=" + partition +
                ", offset=" + offset +
                ", timestamp=" + timestamp +
                ", serializedValueSize=" + serializedValueSize +
                '}';
    }
}
