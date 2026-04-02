package com.taosdata.jdbc.tmq;

import java.util.Map;

public class TMQEnhMap {
    private final String tableName;
    private final Map<String, Object> map;

    public TMQEnhMap(String tableName, Map<String, Object> map) {
        this.tableName = tableName;
        this.map = map;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, Object> getMap() {
        return map;
    }
}
