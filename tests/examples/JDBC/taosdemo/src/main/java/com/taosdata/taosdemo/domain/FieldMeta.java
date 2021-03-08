package com.taosdata.taosdemo.domain;

import lombok.Data;

@Data
public class FieldMeta {
    private String name;
    private String type;

    public FieldMeta() {
    }

    public FieldMeta(String name, String type) {
        this.name = name;
        this.type = type;
    }
}