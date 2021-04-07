package com.taosdata.tsync;

public class ProducerRecord {
    private final String topic;
    private final int partition;
    private final String message;

    public ProducerRecord(String topic, int partition, String message) {
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

    public String getMessage() {
        return message;
    }
}
