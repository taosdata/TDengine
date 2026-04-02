package com.taosdata.jdbc.ws.tmq.meta;

public enum AlterType {
    ADD_TAG(1),
    DROP_TAG(2),
    RENAME_TAG_NAME(3),
    SET_TAG(4),
    ADD_COLUMN(5),
    DROP_COLUMN(6),
    MODIFY_COLUMN_LENGTH(7),
    MODIFY_TAG_LENGTH(8),
    MODIFY_TABLE_OPTION(9),
    RENAME_COLUMN_NAME(10);
    private final int value;

    AlterType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }
}
