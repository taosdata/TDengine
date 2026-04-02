package com.taosdata.jdbc.ws.tmq.meta;

import java.util.HashMap;
import java.util.Map;

public enum TableType {
    SUPER,
    CHILD,
    NORMAL;

    private static final Map<String, TableType> VALUE_MAP;

    static {
        VALUE_MAP = new HashMap<>(values().length);
        for (TableType type : values()) {
            VALUE_MAP.put(type.name().toUpperCase(), type);
        }
    }

    public static TableType fromString(String value) {
        if (value == null) {
            return null;
        }

        TableType type = VALUE_MAP.get(value.toUpperCase());
        if (type == null) {
            throw new IllegalArgumentException("Invalid TableType value: " + value);
        }
        return type;
    }

    public boolean matches(String value) {
        return this == fromString(value);
    }
}
