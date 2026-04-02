package com.taosdata.jdbc.enums;

public enum SchemalessTimestampType {
    // Let the database decide
    NOT_CONFIGURED(""),
    HOURS("h"),
    MINUTES("m"),
    SECONDS("s"),
    MILLI_SECONDS("ms"),
    MICRO_SECONDS("u"),
    NANO_SECONDS("ns"),
    ;

    private final String type;

    SchemalessTimestampType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }
}
