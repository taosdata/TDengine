package com.taosdata.tsync.entity.produceJob;


import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class TaskConfiguration extends Configuration {
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