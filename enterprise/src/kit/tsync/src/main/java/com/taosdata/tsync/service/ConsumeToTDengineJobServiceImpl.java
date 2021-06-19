package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.factory.TaosdConnectionFactory;
import com.taosdata.tsync.factory.WriteToTDengineTaskFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.repository.RunnableTaskRepository;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToTDengineJobServiceImpl extends AbstractJobService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToTDengineJobServiceImpl.class);
    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
    private final RunnableTaskRepository runnableTaskRepository = RunnableTaskRepository.getInstance();

    private String topic;
    private int[] partitions;
    private int threadSize;
    private Multimap<Integer, Integer> threadIndex2PartitionList;

    public ConsumeToTDengineJobServiceImpl() {
        super();
    }

    @Override
    public List<Integer> prepare(ConfigurationType configurationType, UUID configurationId) throws Exception {
        ConsumeToTDengineConfiguration configuration = (ConsumeToTDengineConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            throw new Exception("cannot find Configuration of id:[" + configurationId + "]");
        }

        // Consumer Configuration ==> consumer, Task Configuration ==> topic, partitions, threads
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        // 1. use all partitions in tqueue if partitions is missing in configuration
        doPartitionsMissingStrategy(taskConfiguration, consumerConfiguration);

        // 2. arrange threads to partitions
        arrangeThreads();

        // 4. destination Configuration ==> taosd
        TaosdConfiguration taosdConfiguration = (TaosdConfiguration) configuration.findFirst(ConfigurationType.TAOSD);
        if (taosdConfiguration == null) {
            throw new Exception("cannot find taosd in configurations");
        }

        // 5. destination Configuration ==> strategy, schema
        StrategyConfiguration strategyConfiguration = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);

        // 6. create threads
        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {

            TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);
            List<Integer> partitionsToWrite = new ArrayList<>(threadIndex2PartitionList.get(i));
            Connection connection = TaosdConnectionFactory.build(taosdConfiguration);
            int pollingInterval = strategyConfiguration.getPollingInterval();
            SchemaMissingStrategy schemaMissing = strategyConfiguration.getSchemaMissing();

            // callable task
            WriteToTDengineRunnableTask runnable = new WriteToTDengineTaskFactory()
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTopic(topic)
                    .setConsumer(consumer)
                    .setTaosdConnection(connection)
                    .setPollingInterval(pollingInterval)
                    .setSchemaMissingStrategy(schemaMissing)
                    .setSchemaConfiguration(schemaConfiguration)
                    .build();

            RunnableTask runnableTask = new RunnableTask(i, runnable);
            runnableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }
        return taskIds;
    }

    private void doPartitionsMissingStrategy(TaskConfiguration taskConfiguration, ConsumerConfiguration consumerConfiguration) throws Exception {
        TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);
        // check topic and partitions
        checkTopicAndPartitions(taskConfiguration, consumer);
        // use all partitions if partitions not set
        topic = taskConfiguration.getTopic();
        partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            partitions = IntStream.range(1, consumer.getTopic(topic).partitions() + 1).toArray();
        }
        threadSize = taskConfiguration.getThreads();
    }

    private void arrangeThreads() {
        threadIndex2PartitionList = Utils.divideArrIntoGroups(partitions, threadSize);
        int actualThreads = threadIndex2PartitionList.keySet().size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;
    }

    @Override
    public void startAndWait(List<Integer> taskIds) throws Exception {
        // each task create a thread
        List<Thread> threads = IntStream.range(0, taskIds.size()).mapToObj(index -> {
            int taskId = taskIds.get(index);
            RunnableTask task = runnableTaskRepository.find(taskId);
            return new Thread(task.getRunnable(), "task-" + taskId);
        }).collect(Collectors.toList());

        // start
        threads.stream().forEach(Thread::start);

        // wait
        for (Thread thread : threads) {
            thread.join();
        }
    }
}
