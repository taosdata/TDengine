package com.taosdata.jdbc.enums;

public enum SchemalessTimestampType {
    // Let the database decide
    NOT_CONFIGURED,
    HOURS,
    MINUTES,
    SECONDS,
    MILLI_SECONDS,
    MICRO_SECONDS,
    NANO_SECONDS,
    ;
}
