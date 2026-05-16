package com.taosdata.tsync.service;

import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;

import java.util.List;
import java.util.UUID;

public class ConsumeToFileJobServiceImpl extends AbstractRunnableJobService {
    @Override
    public List<UUID> prepare(ConfigurationType configurationType, UUID jobConfigurationId) throws TsyncException {
        return null;
    }

    @Override
    public void shutdown() {

    }
}
