package com.taosdata.taosdemo.domain;

import lombok.Data;

@Data
public class TagValue<T> {
    private String name;
    private T value;

    public TagValue() {
    }

    public TagValue(String name, T value) {
        this.name = name;
        this.value = value;
    }
}
