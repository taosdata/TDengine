package com.taosdata.taosdemo.domain;

import lombok.Data;

import java.util.List;

@Data
public class TableValue {

    private String database;
    private String name;
    private List<FieldMeta> columns;
    private List<RowValue> values;

}
