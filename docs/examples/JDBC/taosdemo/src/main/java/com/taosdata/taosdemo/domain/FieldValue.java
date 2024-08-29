package com.taosdata.taosdemo.domain;

import lombok.Data;

@Data
public class FieldValue<T> {
    private String name;
    private T value;

    public FieldValue() {
    }

    public FieldValue(String name, T value) {
        this.name = name;
        this.value = value;
    }
}
