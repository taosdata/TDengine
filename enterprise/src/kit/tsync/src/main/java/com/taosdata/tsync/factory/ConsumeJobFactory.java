package com.taosdata.tsync.factory;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.repository.ConfigurationRepository;

public class ConsumeJobFactory {

    public static Job build(JSONObject configJSON, ConfigurationRepository configurationRepository) {
        Configuration configuration = ConfigurationFactory.build(ConfigurationType.CONSUME_JOB, configJSON);
        configurationRepository.add(configuration);
        return new Job(ConfigurationType.CONSUME_JOB, configuration.getId());
    }
}
