package com.taosdata.tsync.entity.config;


import com.taosdata.tsync.enums.ConfigurationType;

public class DestinationConfiguration extends Configuration {

    public DestinationConfiguration() {
        super(ConfigurationType.DESTINATION);
    }
}
