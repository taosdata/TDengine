package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class MessageConfiguration extends Configuration {

    public static final Long DEFAULT_BATCH_VALUES = 1L;
    public static final Long DEFAULT_BATCH_TABLES = 1L;

    private Long total;
    private Long batchValues;
    private Long batchTables;
    private Long startTime;

    public MessageConfiguration() {
        super(ConfigurationType.MESSAGE);
    }

    // getter and setter
    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public Long getBatchValues() {
        return batchValues;
    }

    public void setBatchValues(Long batchValues) {
        this.batchValues = batchValues;
    }

    public Long getBatchTables() {
        return batchTables;
    }

    public void setBatchTables(Long batchTables) {
        this.batchTables = batchTables;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }
}