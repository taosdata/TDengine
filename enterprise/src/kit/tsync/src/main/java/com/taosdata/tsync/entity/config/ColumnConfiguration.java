package com.taosdata.tsync.entity.config;

public class ColumnConfiguration extends Configuration {

    private String name;
    private String type;
    private Integer length;

    public ColumnConfiguration() {
        super(ConfigurationType.COLUMN);
    }

    // getter and setter
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }
}