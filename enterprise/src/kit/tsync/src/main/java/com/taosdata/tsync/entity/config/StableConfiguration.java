package com.taosdata.tsync.entity.config;

import com.taosdata.tsync.enums.ConfigurationType;

public class StableConfiguration extends Configuration {
    private String name;
    private Long tables;

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

    public Long getTables() {
        return tables;
    }

    public void setTables(Long tables) {
        this.tables = tables;
    }

}
