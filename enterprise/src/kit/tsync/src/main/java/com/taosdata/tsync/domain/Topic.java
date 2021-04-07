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

}
