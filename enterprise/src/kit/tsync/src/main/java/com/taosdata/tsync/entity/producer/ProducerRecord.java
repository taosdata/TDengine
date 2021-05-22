package com.taosdata.tsync.entity.producer;

public class ProducerRecord<T> {
    private final String topic;
    private final int partition;
    private final T message;

    public ProducerRecord(String topic, int partition, T message) {
        this.topic = topic;
        this.partition = partition;
        this.message = message;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public T getMessage() {
        return message;
    }
}
