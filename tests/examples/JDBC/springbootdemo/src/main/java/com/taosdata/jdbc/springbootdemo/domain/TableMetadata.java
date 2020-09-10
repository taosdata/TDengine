package com.taosdata.jdbc.springbootdemo.domain;

import java.util.List;

public class TableMetadata {

    private String dbname;
    private String tablename;
    private List<FieldMetadata> fields;
    private List<TagMetadata> tags;

    public String getDbname() {
        return dbname;
    }

    public void setDbname(String dbname) {
        this.dbname = dbname;
    }

    public String getTablename() {
        return tablename;
    }

    public void setTablename(String tablename) {
        this.tablename = tablename;
    }

    public List<FieldMetadata> getFields() {
        return fields;
    }

    public void setFields(List<FieldMetadata> fields) {
        this.fields = fields;
    }

    public List<TagMetadata> getTags() {
        return tags;
    }

    public void setTags(List<TagMetadata> tags) {
        this.tags = tags;
    }
}
