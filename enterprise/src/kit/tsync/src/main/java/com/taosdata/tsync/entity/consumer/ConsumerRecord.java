package com.taosdata.tsync.entity.consumer;

public class ConsumerRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final long ts;
    private final byte[] value;

    public ConsumerRecord(String topic, int partition, long offset, long ts, byte[] value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.ts = ts;
        this.value = value;
    }

    public String topic() {
        return this.topic;
    }

    public int partition() {
        return this.partition;
    }

    public long offset() {
        return this.offset;
    }

    public byte[] value() {
        return this.value;
    }
}
