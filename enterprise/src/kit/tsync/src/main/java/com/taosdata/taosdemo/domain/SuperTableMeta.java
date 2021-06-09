package com.taosdata.taosdemo.domain;

import java.util.List;

public class SuperTableMeta {

    private String database;
    private String name;
    private List<FieldMeta> fields;
    private List<TagMeta> tags;

    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<FieldMeta> getFields() {
        return fields;
    }

    public void setFields(List<FieldMeta> fields) {
        this.fields = fields;
    }

    public List<TagMeta> getTags() {
        return tags;
    }

    public void setTags(List<TagMeta> tags) {
        this.tags = tags;
    }
}