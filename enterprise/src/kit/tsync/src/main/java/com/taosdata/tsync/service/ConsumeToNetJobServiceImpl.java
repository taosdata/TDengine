package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConsumeToNetRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToNetJobServiceImpl extends AbstractRunnableJobService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToNetJobServiceImpl.class);

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {

        Configuration config = configurationRepository.find(configurationId);
        if (config == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        ConsumeToNetConfiguration configuration = (ConsumeToNetConfiguration) config;

        // 1. Consumer Configuration ==> consumer
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        final TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);

        // 2. Task Configuration ==> topic, partitions, threads
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        final String topic = taskConfiguration.getTopic();
        if (topic == null || !consumer.containsTopic(topic)) {
            String errMsg = "topic[" + topic + "] does not exist";
            logger.error(errMsg);
            throw new TsyncException(errMsg);
        }
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            String errorMsg = "partition is null or partition.length is 0";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        if (containsInvalidPartitionIndex(partitions, consumer.getTopic(topic).partitions())) {
            String errorMsg = "partitions contains invalid partition index";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        final List<Integer> partitionsToWrite = IntStream.of(partitions).boxed().collect(Collectors.toList());

        // 3. polling interval
        StrategyConfiguration strategy = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        final int pollingInterval = strategy.getPollingInterval();

        // 4. destination configuration ==> net
        NetConfiguration netConfiguration = (NetConfiguration) configuration.findFirst(ConfigurationType.NET);
        final String host = netConfiguration.getHost();
        final int port = netConfiguration.getPort();

        // 5. create threads
        ConsumeToNetRunnableTask consumeToNetRunnableTask = new ConsumeToNetRunnableTaskFactory()
                .setConsumer(consumer)
                .setTopic(topic)
                .setPartitionsToWrite(partitionsToWrite)
                .setPollingInterval(pollingInterval)
                .setHost(host)
                .setPort(port)
                .build();
        RunnableTask task = new RunnableTask(consumeToNetRunnableTask);
        runnableTaskRepository.add(task);
        // return
        List<UUID> taskIds = new ArrayList<>();
        taskIds.add(task.getId());
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
