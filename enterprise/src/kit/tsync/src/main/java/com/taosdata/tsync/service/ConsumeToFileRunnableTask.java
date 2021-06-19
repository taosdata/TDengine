package com.taosdata.tsync.service;

import com.taosdata.tsync.tqueue.TQueueConsumer;

import java.io.File;
import java.util.List;

public class ConsumeToFileRunnableTask implements Runnable {

    private List<Integer> partitionsToWrite;
    private String topic;
    private TQueueConsumer consumer;
    private int pollingInterval;
    private File directory;

    @Override
    public void run() {

    }

    public void setPartitionsToWrite(List<Integer> partitionsToWrite) {
        this.partitionsToWrite = partitionsToWrite;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setConsumer(TQueueConsumer consumer) {
        this.consumer = consumer;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public void setDirectory(File directory) {
        this.directory = directory;
    }
}
