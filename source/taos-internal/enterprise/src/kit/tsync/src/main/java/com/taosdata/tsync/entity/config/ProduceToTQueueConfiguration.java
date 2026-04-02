package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class ProduceToTQueueConfiguration extends Configuration {

    public ProduceToTQueueConfiguration() {
        super(ConfigurationType.PRODUCE_TO_TQUEUE);
    }
}