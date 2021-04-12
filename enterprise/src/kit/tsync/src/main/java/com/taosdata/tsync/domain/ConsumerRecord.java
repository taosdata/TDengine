package com.taosdata.tsync.domain;

public class ConsumerRecord {

    private final String topic;
    private final int partition;
    private final long offset;
    private final String value;

    public ConsumerRecord(String topic, int partition, long offset, String value) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
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

    public String value() {
        return this.value;
    }
}
