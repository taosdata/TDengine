package com.taosdata.tsync.factory;

import com.taosdata.tsync.service.ConsumeToFileRunnableTask;
import com.taosdata.tsync.tqueue.TQueueConsumer;

import java.io.File;
import java.util.List;

public class ConsumeToFileRunnableTaskFactory {

    private final ConsumeToFileRunnableTask instance;

    public ConsumeToFileRunnableTask build() {
        return instance;
    }

    public ConsumeToFileRunnableTaskFactory() {
        instance = new ConsumeToFileRunnableTask();
    }

    public ConsumeToFileRunnableTaskFactory setConsumer(TQueueConsumer consumer) {
        instance.setConsumer(consumer);
        return this;
    }

    public ConsumeToFileRunnableTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public ConsumeToFileRunnableTaskFactory setPartitionsToWrite(List<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public ConsumeToFileRunnableTaskFactory setPollingInterval(int pollingInterval) {
        instance.setPollingInterval(pollingInterval);
        return this;
    }

    public ConsumeToFileRunnableTaskFactory setDirectory(File directory) {
        instance.setDirectory(directory);
        return this;
    }
}
