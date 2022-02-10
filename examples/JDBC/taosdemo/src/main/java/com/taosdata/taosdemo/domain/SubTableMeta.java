package com.taosdata.taosdemo.domain;

import lombok.Data;

import java.util.List;

@Data
public class SubTableMeta {

    private String database;
    private String supertable;
    private String name;
    private List<TagValue> tags;
    private List<FieldMeta> fields;
}
