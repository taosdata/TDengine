package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;
import com.taosdata.tsync.enums.SchemaMissingStrategy;

public class StrategyConfiguration extends Configuration {
    private int pollingInterval;
    private SchemaMissingStrategy schemaMissing;

    public StrategyConfiguration() {
        super(ConfigurationType.STRATEGY);
    }

    //getter and setter
    public int getPollingInterval() {
        return pollingInterval;
    }

    public void setPollingInterval(int pollingInterval) {
        this.pollingInterval = pollingInterval;
    }

    public SchemaMissingStrategy getSchemaMissing() {
        return schemaMissing;
    }

    public void setSchemaMissing(SchemaMissingStrategy schemaMissing) {
        this.schemaMissing = schemaMissing;
    }
}
