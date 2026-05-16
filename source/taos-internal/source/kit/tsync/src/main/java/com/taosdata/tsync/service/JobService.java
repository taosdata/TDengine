package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

import java.util.List;
import java.util.UUID;

public interface JobService {

    List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException;

    void startAndWait(List<UUID> taskIds) throws TsyncException;

    void shutdown();

    Configuration getConfiguration(ConfigurationType configurationType, UUID configurationId);

}