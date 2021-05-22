package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class StableConfiguration extends Configuration {
    private String name;
    private Integer tables;

    public StableConfiguration() {
        super(ConfigurationType.STABLE);
    }

    // getter and setter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getTables() {
        return tables;
    }

    public void setTables(Integer tables) {
        this.tables = tables;
    }

}
