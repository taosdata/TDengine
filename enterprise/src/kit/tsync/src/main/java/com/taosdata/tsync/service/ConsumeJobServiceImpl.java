package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.taosdata.tsync.TQueueConsumer;
import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
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
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeJobServiceImpl extends AbstractJobService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeJobServiceImpl.class);
    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
    private final RunnableTaskRepository runnableTaskRepository = RunnableTaskRepository.getInstance();

    public ConsumeJobServiceImpl() {
        super();
    }

    @Override
    public List<Integer> prepare(ConfigurationType configurationType, UUID configurationId) throws Exception {
        ConsumeJobConfiguration configuration = (ConsumeJobConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            throw new Exception("cannot find Configuration of id:[" + configurationId + "]");
        }
        // 1. Consumer Configuration ==> consumer
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);

        // 2. Task Configuration ==> topic, partitions, threads
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        // check topic and partitions
        checkTopicAndPartitions(taskConfiguration, consumer);
        // use all partitions if partitions not set
        String topic = taskConfiguration.getTopic();
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            partitions = IntStream.range(1, consumer.getTopic(topic).partitions() + 1).toArray();
        }
        int threads = taskConfiguration.getThreads();

        // 3. arrange threads and partitions
        Multimap<Integer, Integer> threadPartitionMultiMap = Utils.divideArrIntoGroups(partitions, threads);
        int actualThreads = threadPartitionMultiMap.keySet().size();
        if (threads > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threads = actualThreads;

        // 4. destination Configuration ==> tdengine
        TaosdConfiguration taosdConfiguration = (TaosdConfiguration) configuration.findFirst(ConfigurationType.TAOSD);
        if (taosdConfiguration == null) {
            throw new Exception("cannot find taosd in configurations");
        }

        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            // callable task
            Collection<Integer> partitionsToWrite = threadPartitionMultiMap.get(i);
            WriteToTDengineRunnableTask runnable = new WriteToTDengineTaskFactory()
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTopic(topic)
                    .setConsumer(consumerConfiguration)
                    .setTaosdConfiguration(taosdConfiguration)
                    .build();

            RunnableTask runnableTask = new RunnableTask(i, runnable);
            runnableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }
        return taskIds;
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
