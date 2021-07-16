package com.taosdata.tsync.service;

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
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ProduceToTQueueJobServiceImpl extends AbstractCallableJobService {
    private static final Logger logger = LoggerFactory.getLogger(ProduceToTQueueJobServiceImpl.class);

    public ProduceToTQueueJobServiceImpl() {
        super(new AffectRowsProcessService());
    }

    private TQueueProducer<String> producer;
    private List<UUID> taskIds = new ArrayList<>();

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
        producer = TQueueProducerFactory.build(producerConfiguration);
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

        List<Integer[]> threadIndex2PartitionList = Utils.divideArrayIntoGroups(partitions, threadSize);

        int actualThreads = threadIndex2PartitionList.size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;

        // arrange tables ==> threads
        StableConfiguration stableConfiguration = (StableConfiguration) configuration.findFirst(ConfigurationType.STABLE);
        long tables = stableConfiguration.getTables();

        List<Range<Long>> tableRanges = Utils.divideIntoRangeList(tables, threadSize);

        // 3. messageConfiguration ==> total record
        MessageConfiguration messageConfiguration = (MessageConfiguration) configuration.findFirst(ConfigurationType.MESSAGE);
        long total = messageConfiguration.getTotal();

        List<Range<Long>> threadRecords = Utils.divideIntoRangeList(total, threadSize);

        long batchTables = messageConfiguration.getBatchTables();
        long batchValues = messageConfiguration.getBatchValues();

        // 6. SchemaConfiguration ==> schema
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);

        // 7. create threads
        for (int i = 0; i < threadSize; i++) {
            // callable task
            int[] partitionsToWrite = Arrays.stream(threadIndex2PartitionList.get(i)).mapToInt(Integer::intValue).toArray();

            Range<Long> tablesToWrite = tableRanges.get(i);

            Range<Long> recordsToWrite = threadRecords.get(i);
            long records = recordsToWrite.upperEndpoint() - recordsToWrite.lowerEndpoint();

            ProduceToTQueueCallableTask callable = new ProduceToTQueueCallableTaskFactory()
                    .setProducer(TQueueProducerFactory.build(producerConfiguration))
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

    @Override
    public void shutdown() {
        producer.close();
        for (UUID taskId : taskIds) {
            ProduceToTQueueCallableTask produceToTQueueCallableTask = (ProduceToTQueueCallableTask) callableTaskRepository.find(taskId).getCallable();
            produceToTQueueCallableTask.shutdown();
        }
    }

    private boolean containsInvalidPartitionIndex(int[] partitions, int bound) {
        for (int partitionIndex : partitions) {
            if (partitionIndex < 1 || partitionIndex > bound)
                return true;
        }
        return false;
    }


}
