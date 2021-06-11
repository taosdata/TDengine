package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.taosdata.tsync.TQueueProducer;
import com.taosdata.tsync.entity.CallableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.TQueueProducerFactory;
import com.taosdata.tsync.factory.WriteToTQueueTaskFactory;
import com.taosdata.tsync.repository.CallableTaskRepository;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProduceJobServiceImpl extends AbstractJobService {
    private static final Logger logger = LoggerFactory.getLogger(ProduceJobServiceImpl.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
    private final CallableTaskRepository callableTaskRepository = CallableTaskRepository.getInstance();
    private final ResultProcessService resultProcessService;

    public ProduceJobServiceImpl(ResultProcessService resultProcessService) {
        super();
        this.resultProcessService = resultProcessService;
    }

    @Override
    public List<Integer> prepare(ConfigurationType configurationType, UUID configurationId) throws Exception {
        // find configuration
        ProduceJobConfiguration configuration = (ProduceJobConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            throw new Exception("cannot find Configuration of id:[" + configurationId + "]");
        }

        // 1. Producer Configuration ==> producer
        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);
        TQueueProducer producer = TQueueProducerFactory.build(producerConfiguration);

        // 2. Task Configuration ==> topic, partitions, threads
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        // check topic and partitions
        checkTopicAndPartitions(taskConfiguration, producer);
        // use all partitions if partitions not set
        String topic = taskConfiguration.getTopic();
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            partitions = IntStream.range(1, producer.getTopic(topic).partitions() + 1).toArray();
        }
        int threads = taskConfiguration.getThreads();
        // arrange threads and partitions
        Multimap<Integer, Integer> threadPartitionMultiMap = Utils.divideArrIntoGroups(partitions, threads);
        int actualThreads = threadPartitionMultiMap.keySet().size();
        if (threads > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threads = actualThreads;

        // 3. StableConfiguration ==> tables
        StableConfiguration stableConfiguration = (StableConfiguration) configuration.findFirst(ConfigurationType.STABLE);
        long tables = stableConfiguration.getTables();
        Map<Integer, Range<Long>> threadTableMap = Utils.divideIntoGroups(tables, threads);

        // 4. messageConfiguration ==> total record
        MessageConfiguration messageConfiguration = (MessageConfiguration) configuration.findFirst(ConfigurationType.MESSAGE);
        long total = messageConfiguration.getTotal();
        Map<Integer, Range<Long>> threadRecordMap = Utils.divideIntoGroups(total, threads);

        // 5. messageConfiguration ==> batchTables, batchValues
        long batchTables = messageConfiguration.getBatchTables();
        long batchValues = messageConfiguration.getBatchValues();

        // 6. SchemaConfiguration ==> schema
        SchemaConfiguration schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);

        // 7. create threads
        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            // callable task
            Collection<Integer> partitionsToWrite = threadPartitionMultiMap.get(i);
            Range<Long> tablesToWrite = threadTableMap.get(i);
            Range<Long> recordsToWrite = threadRecordMap.get(i);
            long records = recordsToWrite.upperEndpoint() - recordsToWrite.lowerEndpoint();
            //TODO: 优化这里的代码结构
            WriteToTQueueCallableTask callable = new WriteToTQueueTaskFactory()
                    .setPartitionsToWrite(partitionsToWrite)
                    .setTablesToWrite(tablesToWrite)
                    .setRecordsToWrite(records)
                    .setBatchTables(batchTables)
                    .setBatchValues(batchValues)
                    .setTopic(topic)
                    .setProducer(producer)
                    .setSchemaConfiguration(schemaConfiguration)
                    .build();

            CallableTask<Integer> runnableTask = new CallableTask(i, callable);
            callableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }

        return taskIds;
    }

    @Override
    public void startAndWait(List<Integer> taskIds) throws Exception {
        List<FutureTask> futureTasks = new ArrayList<>();
        List<Thread> threads = IntStream.range(0, taskIds.size()).mapToObj(i -> {
            // each task create a thread
            int taskId = taskIds.get(i);
            CallableTask task = callableTaskRepository.find(taskId);
            FutureTask futureTask = new FutureTask<>(task.getCallable());
            futureTasks.add(futureTask);
            return new Thread(futureTask, "task-" + taskId);
        }).collect(Collectors.toList());
        // start
        threads.stream().forEach(Thread::start);
        // wait
        for (Thread thread : threads) {
            thread.join();
        }
        // get result
        for (FutureTask task : futureTasks) {
            Object result = task.get();
            resultProcessService.process(result);
        }
        Object result = resultProcessService.getResult();
        logger.info("get result: " + result.toString());
    }


}
