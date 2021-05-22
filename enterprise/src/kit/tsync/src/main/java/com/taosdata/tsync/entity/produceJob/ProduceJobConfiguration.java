package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class ProduceJobConfiguration extends Configuration {

    public ProduceJobConfiguration() {
        super(ConfigurationType.PRODUCE_JOB);
    }
}
