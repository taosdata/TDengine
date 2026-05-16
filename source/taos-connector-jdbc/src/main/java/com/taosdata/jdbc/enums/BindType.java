package com.taosdata.jdbc.enums;

public enum BindType {
    TAG(1),
    BIND(2),
    ;
    private final long length;

    BindType(int length) {
        this.length = length;
    }

    public long get() {
        return length;
    }
}
