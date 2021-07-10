package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConsumeToNetRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

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

        // 3. polling interval
        StrategyConfiguration strategy = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        final int pollingInterval = strategy.getPollingInterval();

        // 4. destination configuration ==> net
        NetConfiguration netConfiguration = (NetConfiguration) configuration.findFirst(ConfigurationType.NET);
        final String host = netConfiguration.getHost();
        final int port = netConfiguration.getPort();

        // 5. create threads
        int threads = taskConfiguration.getThreads();
        List<Integer[]> thread2Partition = Utils.divideArrayIntoGroups(partitions, threads);

        List<UUID> taskIds = new ArrayList<>();
        thread2Partition.stream().map(partitionsToConsume -> {
            int[] partitionsToConsumeArr = Arrays.stream(partitionsToConsume).mapToInt(i -> i).toArray();
            return new ConsumeToNetRunnableTaskFactory()
                    .setConsumer(TQueueConsumerFactory.build(consumerConfiguration))
                    .setTopic(topic)
                    .setPartitionsToWrite(partitionsToConsumeArr)
                    .setPollingInterval(pollingInterval)
                    .setHost(host)
                    .setPort(port)
                    .build();
        }).forEach(consumeToNetRunnableTask -> {
            RunnableTask task = new RunnableTask(consumeToNetRunnableTask);
            runnableTaskRepository.add(task);
            taskIds.add(task.getId());
        });

        return taskIds;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

    private boolean containsInvalidPartitionIndex(int[] partitions, int bound) {
        for (int partitionIndex : partitions) {
            if (partitionIndex < 1 || partitionIndex > bound)
                return true;
        }
        return false;
    }


}
