package com.taosdata.taosdemo.domain;

import lombok.Data;

import java.util.List;

@Data
public class RowValue {
    private List<FieldValue> fields;


    public RowValue(List<FieldValue> fields) {
        this.fields = fields;
    }
}