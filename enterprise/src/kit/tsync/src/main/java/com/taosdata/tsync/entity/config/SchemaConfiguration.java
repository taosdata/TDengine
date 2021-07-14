package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class SchemaConfiguration extends Configuration {

    private Long startTime;

    public SchemaConfiguration() {
        super(ConfigurationType.SCHEMA);
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}
