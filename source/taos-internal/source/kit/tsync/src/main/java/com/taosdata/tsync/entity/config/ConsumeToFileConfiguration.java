package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ConsumeToFileConfiguration extends Configuration {

    public ConsumeToFileConfiguration() {
        super(ConfigurationType.CONSUME_TO_FILE);
    }
}
