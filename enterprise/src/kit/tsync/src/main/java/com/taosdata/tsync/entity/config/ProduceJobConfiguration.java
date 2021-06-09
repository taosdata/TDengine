package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ProduceJobConfiguration extends Configuration {

    public ProduceJobConfiguration() {
        super(ConfigurationType.PRODUCE_JOB);
    }
}