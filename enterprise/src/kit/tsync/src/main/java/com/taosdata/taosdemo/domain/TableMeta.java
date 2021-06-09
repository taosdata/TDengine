package com.taosdata.taosdemo.domain;

import java.util.List;

public class TableMeta {

    private String database;
    private String name;
    private List<FieldMeta> fields;

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
}
