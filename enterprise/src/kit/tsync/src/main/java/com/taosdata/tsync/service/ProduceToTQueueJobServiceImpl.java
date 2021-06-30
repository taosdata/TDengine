package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.taosdata.tsync.entity.CallableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ProduceToTQueueCallableTaskFactory;
import com.taosdata.tsync.factory.TQueueProducerFactory;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

public class ProduceToTQueueJobServiceImpl extends AbstractCallableJobService {
    private static final Logger logger = LoggerFactory.getLogger(ProduceToTQueueJobServiceImpl.class);

    private String topic;
    private int threadSize;
    private Multimap<Integer, Integer> threadIndex2PartitionList;
    private Map<Integer, Range<Long>> threadIndex2TableRange;
    private Map<Integer, Range<Long>> threadRecordMap;
    private long batchTables;
    private long batchValues;
    private SchemaConfiguration schemaConfiguration;

    public ProduceToTQueueJobServiceImpl() {
        super(new AffectRowsProcessService());
    }

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {
        // find configuration
        ProduceToTQueueConfiguration configuration = (ProduceToTQueueConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // 1. do partition missing strategy
        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        TQueueProducer producer_one = TQueueProducerFactory.build(producerConfiguration);
        // check topic and partitions
        checkTopicAndPartitions(taskConfiguration, producer_one);
        // use all partitions if partitions not set
        topic = taskConfiguration.getTopic();
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            partitions = IntStream.range(1, producer_one.getTopic(topic).partitions() + 1).toArray();
        }
        threadSize = taskConfiguration.getThreads();
        // arrange threads and partitions
        threadIndex2PartitionList = Utils.divideArrIntoGroups(partitions, threadSize);
        int actualThreads = threadIndex2PartitionList.keySet().size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;

        // 2. StableConfiguration ==> tables
        StableConfiguration stableConfiguration = (StableConfiguration) configuration.findFirst(ConfigurationType.STABLE);
        long tables = stableConfiguration.getTables();
        threadIndex2TableRange = Utils.divideIntoGroups(tables, threadSize);

        // 3. messageConfiguration ==> total record
        MessageConfiguration messageConfiguration = (MessageConfiguration) configuration.findFirst(ConfigurationType.MESSAGE);
        long total = messageConfiguration.getTotal();
        threadRecordMap = Utils.divideIntoGroups(total, threadSize);
        batchTables = messageConfiguration.getBatchTables();
        batchValues = messageConfiguration.getBatchValues();
        // 6. SchemaConfiguration ==> schema
        schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);

        // 7. create threads
        List<UUID> taskIds = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            // callable task
            List<Integer> partitionsToWrite = new ArrayList<>(threadIndex2PartitionList.get(i));
            Range<Long> tablesToWrite = threadIndex2TableRange.get(i);
            Range<Long> recordsToWrite = threadRecordMap.get(i);
            long records = recordsToWrite.upperEndpoint() - recordsToWrite.lowerEndpoint();
            TQueueProducer producer = TQueueProducerFactory.build(producerConfiguration);

            ProduceToTQueueCallableTask callable = new ProduceToTQueueCallableTaskFactory()
                    .setProducer(producer)
                    .setTopic(topic)
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTablesToWrite(tablesToWrite)
                    .setRecordsToWrite(records)
                    .setBatchTables(batchTables)
                    .setBatchValues(batchValues)
                    .setSchemaConfiguration(schemaConfiguration)
                    .build();

            CallableTask<Integer> runnableTask = new CallableTask(callable);
            callableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }

        return taskIds;
    }

}
