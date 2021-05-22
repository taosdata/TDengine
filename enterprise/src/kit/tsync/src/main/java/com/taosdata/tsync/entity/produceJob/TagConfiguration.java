package com.taosdata.tsync.entity.produceJob;

import com.taosdata.tsync.entity.Configuration;
import com.taosdata.tsync.entity.ConfigurationType;

public class TagConfiguration extends Configuration {
    private String name;
    private String type;
    private Integer Length;

    public TagConfiguration() {
        super(ConfigurationType.TAG);
    }

    //getter and setter
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
        return Length;
    }

    public void setLength(Integer length) {
        Length = length;
    }
}