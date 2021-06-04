package com.taosdata.tsync.entity.config;

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
