package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.RunnableTask;
import com.taosdata.tsync.entity.config.*;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.factory.ConsumeToFileRunnableTaskFactory;
import com.taosdata.tsync.factory.TQueueConsumerFactory;
import com.taosdata.tsync.repository.ConfigurationRepository;
import com.taosdata.tsync.tqueue.TQueueConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ConsumeToFileJobServiceImpl extends AbstractRunnableJobService {

    private static final Logger logger = LoggerFactory.getLogger(ConsumeToFileJobServiceImpl.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

    public ConsumeToFileJobServiceImpl() {
        super();
    }

    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID configurationId) throws TsyncException {
        Configuration config = configurationRepository.find(configurationId);
        if (config == null) {
            String errorMsg = "cannot find Configuration of id:[" + configurationId + "]";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        ConsumeToFileConfiguration configuration = (ConsumeToFileConfiguration) config;

        // Consumer Configuration ==> consumer, Task Configuration ==> topic, partitions, threads
        ConsumerConfiguration consumerConfiguration = (ConsumerConfiguration) configuration.findFirst(ConfigurationType.CONSUMER);
        TQueueConsumer consumer = TQueueConsumerFactory.build(consumerConfiguration);

        TaskConfiguration taskConfiguration = (TaskConfiguration) configuration.findFirst(ConfigurationType.TASK);
        final String topic = taskConfiguration.getTopic();
        //TODO: check topic
        final int[] partitions = taskConfiguration.getPartitions();
        //TODO: check partitions

        // 2. partitions to write
        List<Integer> partitionsToWrite = IntStream.of(partitions).boxed().collect(Collectors.toList());

        // 3. destination Configuration ==> file
        FileConfiguration fileConfiguration = (FileConfiguration) configuration.findFirst(ConfigurationType.FILE);
        if (fileConfiguration == null) {
            String errorMsg = "cannot find file in configurations";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }
        File directory = new File(fileConfiguration.getDirectory());
        if (!directory.exists() || !directory.isDirectory()) {
            String errorMsg = "file's directory is invalid";
            logger.error(errorMsg);
            throw new TsyncException(errorMsg);
        }

        // 4. destination Configuration ==> strategy ==> polling interval
        StrategyConfiguration strategyConfiguration = (StrategyConfiguration) configuration.findFirst(ConfigurationType.STRATEGY);
        int pollingInterval = strategyConfiguration.getPollingInterval();

        // 5. create threads
        List<UUID> taskIds = new ArrayList<>();

        // callable task
        ConsumeToFileRunnableTask runnable = new ConsumeToFileRunnableTaskFactory()
                .setConsumer(consumer)
                .setTopic(topic)
                .setPartitionsToWrite(partitionsToWrite)
                .setPollingInterval(pollingInterval)
                .setDirectory(directory)
                .build();

        RunnableTask runnableTask = new RunnableTask(runnable);
        runnableTaskRepository.add(runnableTask);
        taskIds.add(runnableTask.getId());
        return taskIds;
    }

    @Override
    public void shutdown() {
        // do nothing
    }

}
