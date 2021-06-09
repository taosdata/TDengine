package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class MessageConfiguration extends Configuration {

    private Long total;
    private Long batchValues;
    private Long batchTables;

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
}