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

public class ProduceToTQueueJobServiceImpl extends AbstractCallableJobService {
    private static final Logger logger = LoggerFactory.getLogger(ProduceToTQueueJobServiceImpl.class);

    public ProduceToTQueueJobServiceImpl() {
        super(new AffectRowsProcessService());
    }

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {
        // throw exception if configuration cannot be found
        ProduceToTQueueConfiguration configuration = (ProduceToTQueueConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);
        TQueueProducer producer = TQueueProducerFactory.build(producerConfiguration);
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        // throw exception if topic not exist
        String topic = taskConfiguration.getTopic();
        if (!producer.containsTopic(topic)) {
            String errMsg = "topic[" + topic + "] does not exist";
            logger.error(errMsg);
            throw new TsyncException(errMsg);
        }

        // throw exception if partitions is null or partitions contains invalid partition index
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            String errorMsg = "partition is null or partition.length is 0";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        if (containsInvalidPartitionIndex(partitions, producer.getTopic(topic).partitions())) {
            String errorMsg = "partitions contains invalid partition index";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // arrange threads ==> partitions
        int threadSize = taskConfiguration.getThreads();
        Multimap<Integer, Integer> threadIndex2PartitionList = Utils.divideArrIntoGroups(partitions, threadSize);
        int actualThreads = threadIndex2PartitionList.keySet().size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;

        // arrange tables ==> threads
        StableConfiguration stableConfiguration = (StableConfiguration) configuration.findFirst(ConfigurationType.STABLE);
        long tables = stableConfiguration.getTables();
        Map<Integer, Range<Long>> threadIndex2TableRange = Utils.divideIntoGroups(tables, threadSize);

        // 3. messageConfiguration ==> total record
        MessageConfiguration messageConfiguration = (MessageConfiguration) configuration.findFirst(ConfigurationType.MESSAGE);
        long total = messageConfiguration.getTotal();
        Map<Integer, Range<Long>> threadRecordMap = Utils.divideIntoGroups(total, threadSize);
        long batchTables = messageConfiguration.getBatchTables();
        long batchValues = messageConfiguration.getBatchValues();
        // 6. SchemaConfiguration ==> schema
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);

        // 7. create threads
        List<UUID> taskIds = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            // callable task
            List<Integer> partitionsToWrite = new ArrayList<>(threadIndex2PartitionList.get(i));
            Range<Long> tablesToWrite = threadIndex2TableRange.get(i);
            Range<Long> recordsToWrite = threadRecordMap.get(i);
            long records = recordsToWrite.upperEndpoint() - recordsToWrite.lowerEndpoint();

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

    private boolean containsInvalidPartitionIndex(int[] partitions, int bound) {
        for (int partitionIndex : partitions) {
            if (partitionIndex < 1 || partitionIndex > bound)
                return true;
        }
        return false;
    }


}
