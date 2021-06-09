package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;

import java.util.List;
import java.util.UUID;

public interface JobService {

    List<Integer> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws Exception;

    void startAndWait(List<Integer> taskIds) throws Exception;

    Configuration getConfiguration(ConfigurationType configurationType, UUID configurationId);

}