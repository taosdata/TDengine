package com.taosdata.tsync.factory;

import com.taosdata.tsync.tqueue.TQueueConsumer;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import com.taosdata.tsync.service.WriteToTDengineRunnableTask;

import java.sql.Connection;
import java.util.List;

public class WriteToTDengineTaskFactory {

    private final WriteToTDengineRunnableTask instance;

    public WriteToTDengineRunnableTask build() {
        return instance;
    }

    public WriteToTDengineTaskFactory() {
        instance = new WriteToTDengineRunnableTask();
    }

    public WriteToTDengineTaskFactory setConsumer(TQueueConsumer consumer) {
        instance.setConsumer(consumer);
        return this;
    }

    public WriteToTDengineTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public WriteToTDengineTaskFactory setPartitionsToWrite(List<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public WriteToTDengineTaskFactory setTaosdConnection(Connection taosdConnection) {
        instance.setTaosdConnection(taosdConnection);
        return this;
    }

    public WriteToTDengineTaskFactory setPollingInterval(int pollingInterval) {
        instance.setPollingInterval(pollingInterval);
        return this;
    }

    public WriteToTDengineTaskFactory setSchemaMissingStrategy(SchemaMissingStrategy schemaMissingStrategy) {
        instance.setSchemaMissing(schemaMissingStrategy);
        return this;
    }

    public WriteToTDengineTaskFactory setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        instance.setSchemaConfiguration(schemaConfiguration);
        return this;
    }
}
