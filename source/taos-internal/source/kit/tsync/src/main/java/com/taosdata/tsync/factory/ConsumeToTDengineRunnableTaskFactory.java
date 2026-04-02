package com.taosdata.tsync.factory;

import com.taosdata.tsync.service.ConsumeToTDengineRunnableTask;
import com.taosdata.tsync.tqueue.TQueueConsumer;

import java.sql.Connection;
import java.util.List;

public class ConsumeToTDengineRunnableTaskFactory {

    private final ConsumeToTDengineRunnableTask instance;

    public ConsumeToTDengineRunnableTask build() {
        return instance;
    }

    public ConsumeToTDengineRunnableTaskFactory() {
        instance = new ConsumeToTDengineRunnableTask();
    }

    public ConsumeToTDengineRunnableTaskFactory setConsumer(TQueueConsumer consumer) {
        instance.setConsumer(consumer);
        return this;
    }

    public ConsumeToTDengineRunnableTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public ConsumeToTDengineRunnableTaskFactory setPartitionsToWrite(List<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public ConsumeToTDengineRunnableTaskFactory setTaosdConnection(Connection taosdConnection) {
        instance.setTaosdConnection(taosdConnection);
        return this;
    }

    public ConsumeToTDengineRunnableTaskFactory setPollingInterval(int pollingInterval) {
        instance.setPollingInterval(pollingInterval);
        return this;
    }

    public ConsumeToTDengineRunnableTaskFactory setStartOffset(long startOffset) {
        instance.setStartOffset(startOffset);
        return this;
    }
}
