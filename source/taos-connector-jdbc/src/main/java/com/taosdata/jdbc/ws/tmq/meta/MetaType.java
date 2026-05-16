package com.taosdata.jdbc.ws.tmq.meta;

import java.util.HashMap;
import java.util.Map;

public enum MetaType {
    // Type: "CREATE" - Create tables, "DROP" - Drop tables, "ALTER" - Alter a table, "DELETE" - Delete data
    CREATE,
    DROP,
    ALTER,
    DELETE;

    private static final Map<String, MetaType> VALUE_MAP;
    static {
        VALUE_MAP = new HashMap<>(values().length);
        for (MetaType type : values()) {
            VALUE_MAP.put(type.name().toUpperCase(), type);
        }
    }

    public static MetaType fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("MetaType value cannot be null");
        }

        MetaType type = VALUE_MAP.get(value.toUpperCase());
        if (type == null) {
            throw new IllegalArgumentException("Invalid MetaType value: " + value);
        }
        return type;
    }

    public boolean matches(String value) {
        return this == fromString(value);
    }
}
