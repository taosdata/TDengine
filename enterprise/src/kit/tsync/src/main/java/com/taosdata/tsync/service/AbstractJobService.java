package com.taosdata.tsync.service;

import com.taosdata.tsync.exceptions.TsyncException;
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

    protected final ConfigurationRepository configurationRepository = ConfigurationRepository.getInstance();

    @Override
    public abstract List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException;

    @Override
    public abstract void startAndWait(List<UUID> taskIds) throws TsyncException;

    @Override
    public Configuration getConfiguration(ConfigurationType configurationType, UUID configurationId) {
        return configurationRepository.find(configurationId);
    }


}
