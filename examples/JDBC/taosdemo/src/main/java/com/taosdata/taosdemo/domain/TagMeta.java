package com.taosdata.taosdemo.domain;

import lombok.Data;

@Data
public class TagMeta {
    private String name;
    private String type;

    public TagMeta() {

    }

    public TagMeta(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
