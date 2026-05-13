package com.taosdata.jdbc.enums;

public enum WSFunction {
    // sql
    WS("ws"),
    // tmq
    TMQ("tmq"),
    //schemaless
    SCHEMALESS("schemaless");

    final String function;

    WSFunction(String function) {
        this.function = function;
    }

    public String getFunction() {
        return function;
    }
}
