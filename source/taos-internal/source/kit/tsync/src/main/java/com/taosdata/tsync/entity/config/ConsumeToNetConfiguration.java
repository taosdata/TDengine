package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ConsumeToNetConfiguration extends Configuration {
    public ConsumeToNetConfiguration() {
        super(ConfigurationType.CONSUME_TO_NET);
    }
}
