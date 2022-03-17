package com.taosdata.taosdemo.domain;

import lombok.Data;

import java.util.List;

@Data
public class SubTableValue {

    private String database;
    private String supertable;
    private String name;
    private List<TagValue> tags;
    private List<RowValue> values;
}
