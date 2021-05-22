package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class SchemaConfiguration extends Configuration {

    public SchemaConfiguration() {
        super(ConfigurationType.SCHEMA);
    }
}
