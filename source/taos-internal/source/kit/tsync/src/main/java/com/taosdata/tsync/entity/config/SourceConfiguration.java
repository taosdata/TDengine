package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class SourceConfiguration extends Configuration {

    public SourceConfiguration() {
        super(ConfigurationType.SOURCE);
    }
}
