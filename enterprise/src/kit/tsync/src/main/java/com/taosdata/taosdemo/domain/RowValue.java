package com.taosdata.taosdemo.domain;

import java.util.List;

public class RowValue {
    private List<FieldValue> fields;

    public RowValue(List<FieldValue> fields) {
        this.fields = fields;
    }

    public List<FieldValue> getFields() {
        return fields;
    }

    public void setFields(List<FieldValue> fields) {
        this.fields = fields;
    }
}