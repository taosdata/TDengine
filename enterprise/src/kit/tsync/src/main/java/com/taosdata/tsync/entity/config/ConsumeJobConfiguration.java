package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ConsumeJobConfiguration extends Configuration {

    public ConsumeJobConfiguration() {
        super(ConfigurationType.CONSUME_JOB);
    }
}
