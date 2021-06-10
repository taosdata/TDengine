package com.taosdata.tsync.service;

import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;

import java.util.List;
import java.util.UUID;

public class ConsumeJobServiceImpl implements JobService {

    @Override
    public List<Integer> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws Exception {
        return null;
    }

    @Override
    public void startAndWait(List<Integer> taskIds) throws Exception {

    }

    @Override
    public Configuration getConfiguration(ConfigurationType configurationType, UUID configurationId) {
        return null;
    }
}
