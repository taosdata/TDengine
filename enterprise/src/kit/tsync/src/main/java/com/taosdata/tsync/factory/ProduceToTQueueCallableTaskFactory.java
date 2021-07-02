package com.taosdata.tsync.factory;

import com.google.common.collect.Range;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.StableConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.service.ProduceToTQueueCallableTask;

import java.util.List;

public class ProduceToTQueueCallableTaskFactory {

    private final ProduceToTQueueCallableTask instance;

    public ProduceToTQueueCallableTaskFactory() {
        instance = new ProduceToTQueueCallableTask();
    }

    public ProduceToTQueueCallableTask build() {
        return instance;
    }

    public ProduceToTQueueCallableTaskFactory setPartitionsToWrite(List<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setTablesToWrite(Range<Long> tablesToWrite) {
        instance.setTablesToWrite(tablesToWrite);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setRecordsToWrite(long recordsToWrite) {
        instance.setRecordsToWrite(recordsToWrite);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setBatchTables(long batchTables) {
        instance.setBatchTables(batchTables);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setBatchValues(long batchValues) {
        instance.setBatchValues(batchValues);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setProducer(TQueueProducer producer) {
        instance.setProducer(producer);
        return this;
    }

    public ProduceToTQueueCallableTaskFactory setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        // dbname
        DatabaseConfiguration databaseConfiguration = (DatabaseConfiguration) schemaConfiguration.findFirst(ConfigurationType.DATABASE);
        String dbname = databaseConfiguration.getName();
        instance.setDbname(dbname);
        // stable
        StableConfiguration stableConfiguration = (StableConfiguration) schemaConfiguration.findFirst(ConfigurationType.STABLE);
        String stableName = stableConfiguration.getName();
        instance.setStableName(stableName);
        // columns
        List<Configuration> columns = stableConfiguration.find(ConfigurationType.COLUMN);
        instance.setColumns(columns);
        // tags
        List<Configuration> tags = stableConfiguration.find(ConfigurationType.TAG);
        instance.setTags(tags);
        return this;
    }
}