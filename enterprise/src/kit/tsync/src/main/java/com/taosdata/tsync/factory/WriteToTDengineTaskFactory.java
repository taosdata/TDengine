package com.taosdata.tsync.factory;

import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.config.ConsumerConfiguration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.StrategyConfiguration;
import com.taosdata.tsync.entity.config.TaosdConfiguration;
import com.taosdata.tsync.service.WriteToTDengineRunnableTask;

import java.sql.Connection;
import java.util.Collection;

public class WriteToTDengineTaskFactory {

    private WriteToTDengineRunnableTask instance;

    public WriteToTDengineRunnableTask build() {
        return instance;
    }

    public WriteToTDengineTaskFactory() {
        instance = new WriteToTDengineRunnableTask();
    }

    public WriteToTDengineTaskFactory setPartitionsToWrite(Collection<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public WriteToTDengineTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public WriteToTDengineTaskFactory setConsumer(ConsumerConfiguration consumerConfiguration) {
        TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);
        instance.setConsumer(consumer);
        return this;
    }

    public WriteToTDengineTaskFactory setPollingInterval(StrategyConfiguration strategyConfiguration) {
        instance.setPollingInterval(strategyConfiguration.getPollingInterval());
        return this;
    }

    public WriteToTDengineTaskFactory setSchemaMissing(StrategyConfiguration strategyConfiguration) {
        instance.setSchemaMissing(strategyConfiguration.getSchemaMissing());
        return this;
    }

    public WriteToTDengineTaskFactory setTaosdConfiguration(TaosdConfiguration taosdConfiguration) {
        Connection connection = TaosdConnectionFactory.build(taosdConfiguration);
        instance.setTaosdConnection(connection);
        return this;
    }

    public WriteToTDengineTaskFactory setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        instance.setSchemaConfiguration(schemaConfiguration);
        return this;
    }
}
