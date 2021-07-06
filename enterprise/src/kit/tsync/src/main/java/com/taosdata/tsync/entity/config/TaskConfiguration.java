package com.taosdata.tsync.entity.config;


import com.taosdata.tsync.enums.ConfigurationType;

public class TaskConfiguration extends Configuration {
    public static final int DEFAULT_THREADS = 1;
    public static final int MAX_PARTITION_INDEX = 1000;
    public static final int MIN_PARTITION_INDEX = 1;

    private int threads;
    private String topic;
    private int[] partitions;

    public TaskConfiguration() {
        super(ConfigurationType.TASK);
    }

    // getter
    public int getThreads() {
        return threads;
    }

    public String getTopic() {
        return topic;
    }

    public int[] getPartitions() {
        return partitions;
    }

    // setter
    public void setThreads(int threads) {
        this.threads = threads;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }
}