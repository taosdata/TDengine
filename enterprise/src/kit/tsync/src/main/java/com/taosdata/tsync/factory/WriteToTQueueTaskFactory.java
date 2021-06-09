package com.taosdata.tsync.factory;

import com.google.common.collect.Range;
import com.taosdata.tsync.TQueueProducer;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.StableConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.service.WriteToTQueueCallableTask;

import java.util.Collection;
import java.util.List;

public class WriteToTQueueTaskFactory {

    private static WriteToTQueueCallableTask instance;

    public WriteToTQueueTaskFactory() {
        instance = new WriteToTQueueCallableTask();
    }

    public WriteToTQueueCallableTask build() {
        return instance;
    }

    public WriteToTQueueTaskFactory setPartitionsToWrite(Collection<Integer> partitionsToWrite) {
        instance.setPartitionsToWrite(partitionsToWrite);
        return this;
    }

    public WriteToTQueueTaskFactory setTablesToWrite(Range<Long> tablesToWrite) {
        instance.setTablesToWrite(tablesToWrite);
        instance.setTables(tablesToWrite.upperEndpoint() - tablesToWrite.lowerEndpoint());
        return this;
    }

    public WriteToTQueueTaskFactory setRecordsToWrite(long recordsToWrite) {
        instance.setRecordsToWrite(recordsToWrite);
        return this;
    }

    public WriteToTQueueTaskFactory setBatchTables(long batchTables) {
        instance.setBatchTables(batchTables);
        return this;
    }

    public WriteToTQueueTaskFactory setBatchValues(long batchValues) {
        instance.setBatchValues(batchValues);
        return this;
    }

    public WriteToTQueueTaskFactory setTopic(String topic) {
        instance.setTopic(topic);
        return this;
    }

    public WriteToTQueueTaskFactory setProducer(TQueueProducer producer) {
        instance.setProducer(producer);
        return this;
    }

    public WriteToTQueueTaskFactory setSchemaConfiguration(SchemaConfiguration schemaConfiguration) {
        instance.setSchemaConfiguration(schemaConfiguration);
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