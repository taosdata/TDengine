package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ConsumeToTDengineConfiguration extends Configuration {

    public ConsumeToTDengineConfiguration() {
        super(ConfigurationType.CONSUME_TO_TDENGINE);
    }
}
