package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class SchemaConfiguration extends Configuration {

    public SchemaConfiguration() {
        super(ConfigurationType.SCHEMA);
    }
}
