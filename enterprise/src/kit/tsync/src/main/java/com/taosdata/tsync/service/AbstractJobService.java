package com.taosdata.tsync.service;

import com.taosdata.tsync.tqueue.TQueueBase;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.entity.config.TaskConfiguration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.repository.ConfigurationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public abstract class AbstractJobService implements JobService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractJobService.class);

    private final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

    @Override
    public abstract List<Integer> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws Exception;

    @Override
    public abstract void startAndWait(List<Integer> taskIds) throws Exception;

    @Override
    public Configuration getConfiguration(ConfigurationType configurationType, UUID configurationId) {
        return configurationRepository.find(configurationId);
    }

    protected void checkTopicAndPartitions(TaskConfiguration taskConfiguration, TQueueBase tqueueBase) throws Exception {
        String topic = taskConfiguration.getTopic();
        // check topic
        if (!tqueueBase.containsTopic(topic)) {
            String errMsg = "topic[" + topic + "] does not exist";
            logger.error(errMsg);
            throw new Exception(errMsg);
        }
        // check partitions
        int[] partitions = taskConfiguration.getPartitions();
        if (partitions != null && !isLegal(partitions, tqueueBase.getTopic(topic).partitions())) {
            String errMsg = "partition:" + Arrays.toString(partitions) + " out of partitions range";
            logger.error(errMsg);
            throw new Exception(errMsg);
        }
    }

    protected boolean isLegal(int[] partitions, int max) {
        for (int i = 0; i < partitions.length; i++) {
            if (partitions[i] < 1 || partitions[i] > max)
                return false;
        }
        return true;
    }
}
