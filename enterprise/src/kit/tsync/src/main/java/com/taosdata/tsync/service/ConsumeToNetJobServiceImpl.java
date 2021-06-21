package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.ConsumerConfig;
import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConsumeToNetRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToNetJobServiceImpl extends AbstractConsumeRunnableJobService {
    private static final Logger logger = LoggerFactory.getLogger(ConsumeToNetJobServiceImpl.class);

    private ConsumerConfiguration consumerConfiguration;
    private TQueueConsumer consumer;
    private TaskConfiguration taskConfiguration;
    private List<Integer> partitionsToWrite;
    private int pollingInterval;
    private String host;
    private int port;

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
        prepareConsumer(configuration);

        // 2. Task Configuration ==> topic, partitions, threads
        prepareTask(configuration);

        // 3. polling interval
        prepareStrategy(configuration);

        // 4. destination configuration ==> net
        prepareNet(configuration);

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

    private void prepareConsumer(ConsumeToNetConfiguration configuration) {
        consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        consumer = TQueueConsumerFactory.build(consumerConfiguration);
    }

    private void prepareTask(ConsumeToNetConfiguration configuration) throws TsyncException {
        taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        // use all partitions in tqueue if partitions is missing in configuration
        super.doPartitionsMissingStrategy(taskConfiguration, consumer);
        partitionsToWrite = IntStream.of(partitions).boxed().collect(Collectors.toList());
    }

    private void prepareNet(ConsumeToNetConfiguration configuration) {
        NetConfiguration netConfiguration = (NetConfiguration) configuration.findFirst(ConfigurationType.NET);
        host = netConfiguration.getHost();
        port = netConfiguration.getPort();
    }

    private void prepareStrategy(ConsumeToNetConfiguration configuration) {
        StrategyConfiguration strategy = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        pollingInterval = strategy.getPollingInterval();
    }

}
