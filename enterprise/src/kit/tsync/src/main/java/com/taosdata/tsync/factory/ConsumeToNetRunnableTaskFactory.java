package com.taosdata.tsync.factory;

import com.taosdata.tsync.service.ConsumeToNetRunnableTask;
import com.taosdata.tsync.tqueue.TQueueConsumer;

import java.util.List;

public class ConsumeToNetRunnableTaskFactory {

    private final ConsumeToNetRunnableTask instance;

    public ConsumeToNetRunnableTask build() {
        return instance;
    }

    public ConsumeToNetRunnableTaskFactory() {
        instance = new ConsumeToNetRunnableTask();
    }

    public ConsumeToNetRunnableTaskFactory setConsumer(TQueueConsumer consumer) {
        instance.setConsumer(consumer);
        return this;
    }

    public ConsumeToNetRunnableTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public ConsumeToNetRunnableTaskFactory setPartitionsToWrite(int[] partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public ConsumeToNetRunnableTaskFactory setPollingInterval(int pollingInterval) {
        instance.setPollingInterval(pollingInterval);
        return this;
    }

    public ConsumeToNetRunnableTaskFactory setHost(String host) {
        instance.setHost(host);
        return this;
    }

    public ConsumeToNetRunnableTaskFactory setPort(int port) {
        instance.setPort(port);
        return this;
    }
}
