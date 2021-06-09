package com.taosdata.taosdemo.domain;

import java.util.List;

public class TableValue {

    private String database;
    private String name;
    private List<FieldMeta> columns;
    private List<RowValue> values;

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

    public List<FieldMeta> getColumns() {
        return columns;
    }

    public void setColumns(List<FieldMeta> columns) {
        this.columns = columns;
    }

    public List<RowValue> getValues() {
        return values;
    }

    public void setValues(List<RowValue> values) {
        this.values = values;
    }
}
