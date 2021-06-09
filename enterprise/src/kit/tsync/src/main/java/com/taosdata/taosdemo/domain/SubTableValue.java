package com.taosdata.taosdemo.domain;

import java.util.List;

public class SubTableValue {

    private String database;
    private String supertable;
    private String name;
    private List<TagValue> tags;
    private List<RowValue> values;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getSupertable() {
        return supertable;
    }

    public void setSupertable(String supertable) {
        this.supertable = supertable;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<TagValue> getTags() {
        return tags;
    }

    public void setTags(List<TagValue> tags) {
        this.tags = tags;
    }

    public List<RowValue> getValues() {
        return values;
    }

    public void setValues(List<RowValue> values) {
        this.values = values;
    }
}
