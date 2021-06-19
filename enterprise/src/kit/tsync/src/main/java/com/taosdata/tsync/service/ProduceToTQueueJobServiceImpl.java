package com.taosdata.tsync.service;

import com.google.common.collect.Multimap;
import com.google.common.collect.Range;
import com.taosdata.tsync.tqueue.TQueueProducer;
import com.taosdata.tsync.entity.CallableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.factory.TQueueProducerFactory;
import com.taosdata.tsync.factory.ProduceToTQueueCallableTaskFactory;
import com.taosdata.tsync.repository.CallableTaskRepository;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ProduceToTQueueJobServiceImpl extends AbstractJobService {
    private static final Logger logger = LoggerFactory.getLogger(ProduceToTQueueJobServiceImpl.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();
    private final CallableTaskRepository callableTaskRepository = CallableTaskRepository.getInstance();
    private final ResultProcessService resultProcessService;

    private String topic;
    private int threadSize;
    private Multimap<Integer, Integer> threadIndex2PartitionList;
    private Map<Integer, Range<Long>> threadIndex2TableRange;
    private Map<Integer, Range<Long>> threadRecordMap;
    private long batchTables;
    private long batchValues;
    private SchemaConfiguration schemaConfiguration;

    public ProduceToTQueueJobServiceImpl(ResultProcessService resultProcessService) {
        super();
        this.resultProcessService = resultProcessService;
    }

    private void arrangeTablesToEachThread(ProduceToTQueueConfiguration configuration) {
        StableConfiguration stableConfiguration = (StableConfiguration) configuration.findFirst(ConfigurationType.STABLE);
        long tables = stableConfiguration.getTables();
        threadIndex2TableRange = Utils.divideIntoGroups(tables, threadSize);
    }

    private void arrangeRecordToEachThread(ProduceToTQueueConfiguration configuration) {
        MessageConfiguration messageConfiguration = (MessageConfiguration) configuration.findFirst(ConfigurationType.MESSAGE);
        long total = messageConfiguration.getTotal();
        threadRecordMap = Utils.divideIntoGroups(total, threadSize);
        batchTables = messageConfiguration.getBatchTables();
        batchValues = messageConfiguration.getBatchValues();
    }

    private void prepareSchema(ProduceToTQueueConfiguration configuration) {
        schemaConfiguration = (SchemaConfiguration) configuration.findFirst(ConfigurationType.SCHEMA);
    }

    @Override
    public List<Integer> prepare(ConfigurationType configurationType, UUID configurationId) throws Exception {
        // find configuration
        ProduceToTQueueConfiguration configuration = (ProduceToTQueueConfiguration) configurationRepository.find(configurationId);
        if (configuration == null) {
            throw new Exception("cannot find Configuration of id:[" + configurationId + "]");
        }

        // 1. do partition missing strategy
        doPartitionMissingStrategy(configuration);
        // 2. StableConfiguration ==> tables
        arrangeTablesToEachThread(configuration);
        // 3. messageConfiguration ==> total record
        arrangeRecordToEachThread(configuration);
        // 6. SchemaConfiguration ==> schema
        prepareSchema(configuration);

        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);

        // 7. create threads
        List<Integer> taskIds = new ArrayList<>();
        for (int i = 0; i < threadSize; i++) {
            // callable task
            List<Integer> partitionsToWrite = new ArrayList<>(threadIndex2PartitionList.get(i));
            Range<Long> tablesToWrite = threadIndex2TableRange.get(i);
            Range<Long> recordsToWrite = threadRecordMap.get(i);
            long records = recordsToWrite.upperEndpoint() - recordsToWrite.lowerEndpoint();
            TQueueProducer producer = TQueueProducerFactory.build(producerConfiguration);

            //TODO: 优化这里的代码结构
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

            CallableTask<Integer> runnableTask = new CallableTask(i, callable);
            callableTaskRepository.add(runnableTask);
            taskIds.add(runnableTask.getId());
        }

        return taskIds;
    }

    private void doPartitionMissingStrategy(ProduceToTQueueConfiguration configuration) throws Exception {
        ProducerConfiguration producerConfiguration = (ProducerConfiguration) configuration.findFirst(ConfigurationType.PRODUCER);
        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        TQueueProducer producer = TQueueProducerFactory.build(producerConfiguration);
        // check topic and partitions
        checkTopicAndPartitions(taskConfiguration, producer);
        // use all partitions if partitions not set
        topic = taskConfiguration.getTopic();
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions == null || partitions.length == 0) {
            partitions = IntStream.range(1, producer.getTopic(topic).partitions() + 1).toArray();
        }
        threadSize = taskConfiguration.getThreads();
        // arrange threads and partitions
        threadIndex2PartitionList = Utils.divideArrIntoGroups(partitions, threadSize);
        int actualThreads = threadIndex2PartitionList.keySet().size();
        if (threadSize > actualThreads) {
            logger.warn("Only " + actualThreads + " threads will be created");
        }
        threadSize = actualThreads;
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
