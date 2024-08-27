package com.taosdata.taosdemo.domain;

import lombok.Data;

import java.util.List;

@Data
public class SuperTableMeta {

    private String database;
    private String name;
    private List<FieldMeta> fields;
    private List<TagMeta> tags;
}