package com.taosdata.tsync.factory;

import com.alibaba.fastjson.JSONObject;
import com.taosdata.tsync.entity.Job;
import com.taosdata.tsync.entity.config.Configuration;
import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.exceptions.TsyncException;
import com.taosdata.tsync.repository.ConfigurationRepository;

public class JobFactory {

    public static Job build(ConfigurationType configurationType, JSONObject configJSON, ConfigurationRepository configurationRepository) {
        Configuration configuration = ConfigurationFactory.build(configurationType, configJSON);
        assert configuration != null;
        configurationRepository.add(configuration);
        return new Job(configurationType, configuration.getId());
    }
}