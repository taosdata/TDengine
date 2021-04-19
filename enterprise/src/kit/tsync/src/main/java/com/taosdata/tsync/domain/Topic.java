package com.taosdata.tsync.domain;

import java.sql.Timestamp;

public class Topic {
    private final String topic;
    private final int partitions;
    private final Timestamp created_time;

    public Topic(String topic, int partitions, Timestamp created_time) {
        this.topic = topic;
        this.partitions = partitions;
        this.created_time = created_time;
    }

    public String topic() {
        return this.topic;
    }

    public int partitions() {
        return this.partitions;
    }

    public Timestamp createTime() {
        return this.created_time;
    }

}
