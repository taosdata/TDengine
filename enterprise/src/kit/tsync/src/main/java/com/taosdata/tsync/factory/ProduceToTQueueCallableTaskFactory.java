package com.taosdata.tsync.factory;

import com.google.common.collect.Range;
import com.taosdata.tsync.enums.DatabasePrecision;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.DatabaseConfiguration;
import com.taosdata.tsync.entity.config.SchemaConfiguration;
import com.taosdata.tsync.entity.config.StableConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.service.ProduceToTQueueCallableTask;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ProduceToTQueueCallableTaskFactory {

    private final ProduceToTQueueCallableTask instance;

    public ProduceToTQueueCallableTaskFactory() {
        instance = new ProduceToTQueueCallableTask();
    }

    public ProduceToTQueueCallableTask build() {
        return instance;
    }

    public ProduceToTQueueCallableTaskFactory setPartitionsToWrite(int[] partitionsToWrite) {
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

        // precision
        DatabasePrecision precision = databaseConfiguration.getPrecision();
        switch (precision) {
            case ns: {
                instance.setTs(new AtomicLong(System.currentTimeMillis() * 1000_000));
                break;
            }
            case us: {
                instance.setTs(new AtomicLong(System.currentTimeMillis() * 1000));
                break;
            }
            case ms:
            default: {
                instance.setTs(new AtomicLong(System.currentTimeMillis()));
                break;
            }
        }

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