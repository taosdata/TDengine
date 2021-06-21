package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.TaskConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.tqueue.TQueueConsumer;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;

public abstract class AbstractConsumeRunnableJobService extends AbstractRunnableJobService {

    protected String topic;
    protected int[] partitions;
    protected int threadSize;

    @Override
    public abstract List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException;

    protected void doPartitionsMissingStrategy(TaskConfiguration taskConfiguration, TQueueConsumer consumer) throws TsyncException {
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

}
