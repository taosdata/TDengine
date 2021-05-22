package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class MessageConfiguration extends Configuration {

    private Long total;
    private Long batchSize;

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

    public Long getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Long batchSize) {
        this.batchSize = batchSize;
    }
}