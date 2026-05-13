package com.taosdata.jdbc.enums;

public enum FieldBindType {
    TAOS_FIELD_COL(1),
    TAOS_FIELD_TAG(2),
    TAOS_FIELD_QUERY(3),
    TAOS_FIELD_TBNAME(4);

    private final int value;

    FieldBindType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static FieldBindType fromValue(int value) {
        for (FieldBindType field : FieldBindType.values()) {
            if (field.value == value) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown value: " + value);
    }
}
