package com.taosdata.jdbc.ws.tmq.meta;

import java.util.Objects;

public abstract class Meta {
    private MetaType type;
    private String tableName;
    private TableType tableType;

    public MetaType getType() {
        return type;
    }

    public void setType(MetaType type) {
        this.type = type;
    }
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public TableType getTableType() {
        return tableType;
    }

    public void setTableType(TableType tableType) {
        this.tableType = tableType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Meta meta = (Meta) o;
        return type == meta.type && Objects.equals(tableName, meta.tableName) && tableType == meta.tableType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, tableName, tableType);
    }
}
